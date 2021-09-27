/*
   Copyright 2011 m6d.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.m6d.filecrush.crush;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.NumberFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;

import com.m6d.filecrush.crush.Bucketer.Bucket;

@SuppressWarnings("deprecation")
public class Crush extends Configured implements Tool {
	/**
	 * Removes the scheme and authority from the path. The path is group 5.
	 */
	private final Matcher pathMatcher = Pattern.compile("([a-z]+:(//[a-z0-9A-Z-]+(\\.[a-z0-9A-Z-]+)*(:[0-9]+)?)?)?(.+)").matcher("dummy");

	/**
	 * The directory within which we find data to crush.
	 */
	private Path srcDir;

	/**
	 * The directory that will store the output of the crush. In normal mode, this is persistent after the job ends. In clone-mode,
	 * this directory is a subdirectory of tmp and will be deleted recursively after the crushed files are moved into the clone dir.
	 */
	private Path outDir;

	private Mode mode;

	/**
	 * In stand alone mode, the the name of the output file. In map reduce mode, the directory to which the crush output files will
	 * be moved. In clone mode, the directory to which the crush input files will be moved after the crush completes.
	 */
	private Path dest;

	/**
	 * The temporary directory that holds {@link #bucketFiles}, {@link #partitionMap}, and {@link #counters}. Deleted recursively.
	 */
	private Path tmpDir;

	/**
	 * The list of directories to crush. Points to a sequence file where the key is the crush output file (aka data bucket) and the
	 * value is a file.
	 */
	private Path bucketFiles;

	/**
	 * The map from directory to partition.
	 *
	 * @see CrushPartitioner
	 */
	private Path partitionMap;

	/**
	 * The counters generated by {@link #writeDirs()}.
	 *
	 * @see CountersMapper
	 */
	private Path counters;

	/**
	 * The maximum size of a file that can be crushed.
	 */
	private long maxEligibleSize;

	/**
	 * Distributed file system block size.
	 */
	private long dfsBlockSize;

	/**
	 * The maximum number of dfs blocks per file.
	 */
	private int maxFileBlocks;

	private JobConf job;

	private FileSystem fs;

	/**
	 * Directory matchers created by --regex command line options. Used to verify that all discovered directories have a crush
	 * specification.
	 */
	private List<Matcher> matchers;
	
	/**
	 * Regex from the --ignore-regex option used for filtering out files for crushing.  
	 */
	private Matcher ignoredFilesMatcher;

	/**
	 * Regex from the --skip-regex option used for skipping files for crushing.  
	 */
	private Matcher skippedFilesMatcher;

	/**
	 * Flag to indicate if empty files should be removed (--remove-empty-files option)
	 */
	private boolean removeEmptyFiles;

	/**
	 * The maximum number of reducer tasks
	 */
	private int maxTasks = 100;

	/**
	 * The counters from the completed job.
	 */
	private Counters jobCounters;

	/**
	 * The codec for the configured compression codec. Used to locate crush output files since Hadoop likes to add things to the
	 * file names you request.
	 */
	private String codecExtension;

	/**
	 * Absolute paths to the skipped files. Path only. No scheme or authority.
	 */
	private Set<String> skippedFiles;

	/**
	 * Absolute paths to the files to be removed. Path only. No scheme or authority.
	 */
	private Set<String> removableFiles;

	/**
	 * The number of crush output files.
	 */
	private int nBuckets;

	/**
	 * Controls whether directories containing single files are eligible for a crush.
	 */
	private boolean excludeSingleFileDirs;

	/**
	 * How much do we want to print to the console.
	 */
	private Verbosity console;

	@SuppressWarnings("static-access")
	Options buildOptions() {
		Options options = new Options();
		Option option;

		option = OptionBuilder
				.withDescription("Print this help message")
				.withLongOpt("help")
				.create("?");
		options.addOption(option);

		option = OptionBuilder
				.hasArg()
				.withArgName("directory regex")
				.withDescription("Regular expression that matches a directory name. Used to match a directory with a correponding replacement string")
				.withLongOpt("regex")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.hasArg()
				.withArgName("ignore file regex")
				.withDescription("Regular expression to apply for filtering out crush candidate files. Any files in the input crush directory matching this will be ignored")
				.withLongOpt("ignore-regex")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.hasArg()
				.withArgName("skip file regex")
				.withDescription("Regular expression to apply for skipping crush candidate files. Any files in the input crush directory matching this will not be considered for crushing")
				.withLongOpt("skip-regex")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.withArgName("remove empty files")
				.withDescription("Remove empty files")
				.withLongOpt("remove-empty-files")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.hasArg()
				.withArgName("replacement string")
				.withDescription("Replacement string used with the corresponding regex to calculate the name of a directory's crush output file")
				.withLongOpt("replacement")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.hasArg()
				.withArgName("FQN input format")
				.withDescription("Input format used to open the files of directories matching the corresponding regex")
				.withLongOpt("input-format")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.hasArg()
				.withArgName("FQN output format")
				.withDescription("Output format used to open the files of directories matching the corresponding regex")
				.withLongOpt("output-format")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.hasArg()
				.withArgName("threshold")
				.withDescription("Threshold relative to the dfs block size over which a file becomes eligible for crushing. Must be in the range (0 and 1]. Default 0.75")
				.withLongOpt("threshold")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.hasArg()
				.withArgName("max-file-blocks")
				.withDescription("The maximum number of dfs blocks per output file. Input files are grouped into output files under the assumption that the input and output compression codecs have comparable efficiency. Default is 8.")
				.withLongOpt("max-file-blocks")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.withDescription("Do not skip directories containing single files.")
				.withLongOpt("include-single-file-dirs")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.hasArg()
				.withArgName("compression codec")
				.withDescription("FQN of the compression codec to use or \"none\". Defaults to DefaultCodec.")
				.withLongOpt("compress")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.withDescription("Operate in clone mode.")
				.withLongOpt("clone")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.hasArg()
				.withDescription("Maximum number of reducer tasks.")
				.withLongOpt("max-tasks")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.hasArg()
				.withDescription("MapReduce job name.")
				.withLongOpt("job-name")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.withDescription("Info logging to console.")
				.withLongOpt("info")
				.create();
		options.addOption(option);

		option = OptionBuilder
				.withDescription("Verbose logging to console.")
				.withLongOpt("verbose")
				.create();
		options.addOption(option);

		return options;
	}

	boolean createJobConfAndParseArgs(String... args) throws ParseException, IOException {

		job = new JobConf(getConf(), Crush.class);

		/*
		 * Turn off speculative execution because that's just wasting network io.
		 */
		job.setMapSpeculativeExecution(false);
		job.setReduceSpeculativeExecution(false);

		/*
		 * Turn off pre-emption because we don't want to kill a task after two hours of network io.
		 */
		job.set("mapred.fairscheduler.preemption", "false");

		tmpDir = new Path("tmp/crush-" + UUID.randomUUID());
		outDir = new Path(tmpDir, "out");

		double threshold = 0.75;

		List<String> regexes			= asList(".+");
		List<String> replacements	= asList("crushed_file-${crush.timestamp}-${crush.task.num}-${crush.file.num}");
		List<String> inFormats		= asList(SequenceFileInputFormat.class.getName());
		List<String> outFormats		= asList(SequenceFileOutputFormat.class.getName());

		String crushTimestamp;

		Options options = buildOptions();
		CommandLine cli = new GnuParser().parse(options, args);

		if (cli.hasOption("?")) {
			BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("help.txt")));

			try {
				String line;

				while (null != (line = reader.readLine())) {
					System.out.println(line);
				}
			} finally {
				reader.close();
			}

			return false;
		}

		if (cli.hasOption("verbose")) {
			console = Verbosity.VERBOSE;
		} else if (cli.hasOption("info")) {
			console = Verbosity.INFO;
		} else {
			console = Verbosity.NONE;
		}
		
		if (cli.hasOption("ignore-regex")) {
			ignoredFilesMatcher = Pattern.compile(cli.getOptionValue("ignore-regex")).matcher("");
		}

		if (cli.hasOption("skip-regex")) {
			skippedFilesMatcher = Pattern.compile(cli.getOptionValue("skip-regex")).matcher("");
		}

		if (cli.hasOption("max-tasks")) {
			maxTasks = Integer.parseInt(cli.getOptionValue("max-tasks"));
		}

		if (cli.hasOption("job-name")) {
			job.set("mapreduce.job.name", cli.getOptionValue("job-name"));
		}

		removeEmptyFiles = cli.hasOption("remove-empty-files");

		excludeSingleFileDirs = !cli.hasOption("include-single-file-dirs");

		String[] nonOptions = cli.getArgs();

		if (2 == nonOptions.length) {
			/*
			 * Stand alone mode accepts two arguments.
			 */
			mode = Mode.STAND_ALONE;

			srcDir = new Path(nonOptions[0]);

			dest = new Path(nonOptions[1]);

			if (cli.hasOption("input-format")) {
				inFormats  = asList(cli.getOptionValue("input-format"));
			}

			if (cli.hasOption("output-format")) {
				outFormats = asList(cli.getOptionValue("output-format"));
			}

			replacements = asList(dest.getName());

			crushTimestamp = Long.toString(currentTimeMillis());

		} else {
			/*
			 * The previous version expected three or four arguments. The third one specified the number of tasks to use, which is an
			 * integral number, just like the third argument in the new version, which is a timestamp. We tell the two apart by looking
			 * at the value of the argument. A timestamp is going to be a huge, 14-digit number while the number of tasks should be much
			 * smaller.
			 */

			if ((args.length == 4 || args.length == 3 ) && args.length == nonOptions.length && args[2].length() != 14) {

				int maxTasks = Integer.parseInt(args[2]);

				if (maxTasks <= 0 || maxTasks > 4000) {
					throw new IllegalArgumentException("Tasks must be in the range [1, 4000]: " + maxTasks);
				}

				job.setInt("mapreduce.job.reduces", maxTasks);

				maxFileBlocks = Integer.MAX_VALUE;

				crushTimestamp = Long.toString(currentTimeMillis());

				srcDir = new Path(args[0]);
				dest   = new Path(args[1]);

				mode = Mode.CLONE;

				if (args.length == 4) {
					if (args[3].equals("TEXT")) {
						/*
						 * These are the defaults except with text input and output formats.
						 */
						inFormats = asList(TextInputFormat.class.getName());
						outFormats = asList(TextOutputFormat.class.getName());

					} else if (!args[3].equals("SEQUENCE")) {
						throw new IllegalArgumentException("Type must be either TEXT or SEQUENCE: " + args[3]);
					}
				}
			} else {
				/*
				 * V2 style arguments.
				 */
				if (cli.hasOption("threshold")) {
					threshold = Double.parseDouble(cli.getOptionValue("threshold"));

					if ( 0 >= threshold || 1 < threshold || Double.isInfinite(threshold) || Double.isNaN(threshold)) {
						throw new IllegalArgumentException("Block size threshold must be in (0, 1]: " + threshold);
					}
				}

				if (cli.hasOption("max-file-blocks")) {
					int maxFileBlocksOption = Integer.parseInt(cli.getOptionValue("max-file-blocks"));

					if (0 > maxFileBlocksOption) {
						throw new IllegalArgumentException("Maximum file size in blocks must be positive: " + maxFileBlocksOption);
					}

					maxFileBlocks = maxFileBlocksOption;
				} else {
					maxFileBlocks = 8;
				}

				if (cli.hasOption("regex")) {
					regexes = asList(cli.getOptionValues("regex"));
				}

				if (cli.hasOption("replacement")) {
					replacements = asList(cli.getOptionValues("replacement"));
				}

				if (cli.hasOption("input-format")) {
					inFormats = asList(cli.getOptionValues("input-format"));
				}

				if (cli.hasOption("output-format")) {
					outFormats = asList(cli.getOptionValues("output-format"));
				}

				if (3 != nonOptions.length) {
					throw new IllegalArgumentException("Could not find source directory, out directory, and job timestamp");
				}

				srcDir = new Path(nonOptions[0]);
				dest   = new Path(nonOptions[1]);

				crushTimestamp	= nonOptions[2];

				if (cli.hasOption("clone")) {
					mode = Mode.CLONE;
				} else {
					mode = Mode.MAP_REDUCE;
				}

				if (!crushTimestamp.matches("\\d{14}")) {
					throw new IllegalArgumentException("Crush timestamp must be 14 digits yyyymmddhhMMss: " + crushTimestamp);
				}
			}

			dfsBlockSize = parseDfsBlockSize(job);
			maxEligibleSize = (long) (dfsBlockSize * threshold);
			print(Verbosity.INFO, format("\nSmall file threshold: " + NumberFormat.getNumberInstance(Locale.US).format(maxEligibleSize) + " bytes\n"));
		}

		/*
		 * Add the crush specs and compression options to the configuration.
		 */
		job.set("crush.timestamp", crushTimestamp);
		
		if (ignoredFilesMatcher != null) {
			job.set("crush.ignore-regex", ignoredFilesMatcher.pattern().pattern());
		}

		if (skippedFilesMatcher != null) {
			job.set("crush.skip-regex", skippedFilesMatcher.pattern().pattern());
		}

		if (regexes.size() != replacements.size() || replacements.size() != inFormats.size() || inFormats.size() != outFormats.size()) {
			throw new IllegalArgumentException("Must be an equal number of regex, replacement, in-format, and out-format options");
		}

		job.setInt("crush.num.specs", regexes.size());

		matchers = new ArrayList<Matcher>(regexes.size());

		for (int i = 0; i < regexes.size(); i++) {
			job.set(format("crush.%d.regex", i), regexes.get(i));

			matchers.add(Pattern.compile(regexes.get(i)).matcher("dummy"));

			job.set(format("crush.%d.regex.replacement", i), replacements.get(i));

			String inFmt = inFormats.get(i);

			if ("sequence".equals(inFmt)) {
				inFmt = SequenceFileInputFormat.class.getName();
			} else if ("text".equals(inFmt)) {
				inFmt = TextInputFormat.class.getName();
			} else if ("avro".equals(inFmt)) {
				inFmt = AvroContainerInputFormat.class.getName();
			} else if ("parquet".equals(inFmt)) {
				inFmt = MapredParquetInputFormat.class.getName();
			} else if ("orc".equals(inFmt)) {
				inFmt = OrcInputFormat.class.getName();
			} else {
				try {
					if (!FileInputFormat.class.isAssignableFrom(Class.forName(inFmt))) {
						throw new IllegalArgumentException("Not a FileInputFormat:" + inFmt);
					}
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException("Not a FileInputFormat:" + inFmt);
				}
			}

			job.set(format("crush.%d.input.format", i), inFmt);

			String outFmt = outFormats.get(i);

			if ("sequence".equals(outFmt)) {
				outFmt = SequenceFileOutputFormat.class.getName();
			} else if ("text".equals(outFmt)) {
				outFmt = TextOutputFormat.class.getName();
			} else if ("avro".equals(outFmt)) {
				outFmt = AvroContainerOutputFormat.class.getName();
			} else if ("parquet".equals(outFmt)) {
				outFmt = MapredParquetOutputFormat.class.getName();
			} else if ("orc".equals(outFmt)) {
				outFmt = OrcOutputFormat.class.getName();
			} else {
				try {
					if (!FileOutputFormat.class.isAssignableFrom(Class.forName(outFmt))) {
						throw new IllegalArgumentException("Not a FileOutputFormat:" + outFmt);
					}
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException("Not a FileOutputFormat:" + outFmt);
				}
			}

			job.set(format("crush.%d.output.format", i), outFmt);
		}

		String codec = cli.getOptionValue("compress");
		String codecClassName = null;

		if (null == codec || "deflate".equals(codec)) {
			codecClassName = DefaultCodec.class.getName();
		} else if ("none".equals(codec)) {
			codecClassName = null;
		} else if ("gzip".equals(codec)) {
			codecClassName = GzipCodec.class.getName();
		} else if ("snappy".equals(codec)) {
			codecClassName = SnappyCodec.class.getName();
		} else if ("bzip2".equals(codec)) {
			codecClassName = BZip2Codec.class.getName();
		} else {
			codecClassName = codec;
			try {
				if (!CompressionCodec.class.isAssignableFrom(Class.forName(codec))) {
					throw new IllegalArgumentException("Not a CompressionCodec: " + codec);
				}
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException("Not a CompressionCodec: " + codec);
			}
		}

		if (null == codecClassName) {
			job.setBoolean("mapreduce.output.fileoutputformat.compress", false);
			job.set("avro.output.codec", "null");
			job.set("parquet.compression", "uncompressed");
			job.set("orc.compress", "NONE");
		} else {
			job.setBoolean("mapreduce.output.fileoutputformat.compress", true);
			job.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
			job.set("mapreduce.output.fileoutputformat.compress.codec", codecClassName);
			job.set("avro.output.codec", codec);
			job.set("parquet.compression", codec);
			job.set("orc.compress", codec);

			try {
				CompressionCodec instance = (CompressionCodec) Class.forName(codecClassName).newInstance();
				codecExtension = instance.getDefaultExtension();
			} catch (Exception e) {
				throw new AssertionError();
			}
		}

		return true;
	}

	/**
	 * The block size has changed over the years... Get with the times.
	 * @param job a conf to check for data
	 * @return a long representing block size
	 * @throws RuntimeException if can not determine block size
	 */
	private long parseDfsBlockSize(JobConf job){
		long old = job.getLong("dfs.blocksize", -1);
		if (old == -1){
			old = job.getLong("dfs.block.size", -1);
		}
		if (old == -1){
			throw new RuntimeException ( "Could not determine how to set block size. Abandon ship!");
		}
		return old;
	}

	@Override
	public int run(String[] args) throws Exception {

		if (!createJobConfAndParseArgs(args)) {
			return 0;
		}

		setFileSystem(FileSystem.get(job));

		FileStatus status = fs.getFileStatus(srcDir);

		if (null == status || !status.isDir()) {
			throw new IllegalArgumentException("No such directory: " + srcDir);
		}

		if (Mode.STAND_ALONE == mode) {
			standAlone();
		} else {
			writeDirs();

			MultipleInputs.addInputPath(job, bucketFiles, SequenceFileInputFormat.class, IdentityMapper.class);
			MultipleInputs.addInputPath(job, counters, CountersInputFormat.class, CountersMapper.class);

			job.setPartitionerClass(CrushPartitioner.class);

			job.setReducerClass(CrushReducer.class);

			job.setOutputKeyComparatorClass(Text.Comparator.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setOutputFormat(SequenceFileOutputFormat.class);

			FileInputFormat.setInputPaths(job, bucketFiles);
			FileOutputFormat.setOutputPath(job, outDir);

			job.set("crush.partition.map", partitionMap.toString());

			if (0 != nBuckets) {
				print(Verbosity.INFO, "\n\nInvoking map reduce\n\n");

				RunningJob completed = JobClient.runJob(job);

				jobCounters = completed.getCounters();
			}

			long eligible = jobCounters.getCounter(MapperCounter.FILES_ELIGIBLE);
			long crushed = jobCounters.getCounter(ReducerCounter.FILES_CRUSHED);

			/*
			 * There's no way this cannot hold true if Hadoop is working correctly.
			 */
			if (eligible != crushed) {
				throw new AssertionError(format("Files eligible (%d) != files crushed (%d)", eligible, crushed));
			}

			if (Mode.CLONE == mode) {
				cloneOutput();
			} else {
				moveOutput();
			}
		}

		print(Verbosity.INFO, "\n\nDeleting temporary directory");

		fs.delete(tmpDir, true);

		/*
		 * If we have printed anything to the console at all, then add a line wrap to bring the cursor back to the beginning.
		 */
		print(Verbosity.INFO, "\n\n");

		return 0;
	}

	private void standAlone() throws IOException {
		String absSrcDir = fs.makeQualified(srcDir).toUri().getPath();
		String absOutDir = fs.makeQualified(outDir).toUri().getPath();

		Text bucket = new Text(absSrcDir + "-0");

		List<Text> files = new ArrayList<Text>();

		FileStatus[] contents = fs.listStatus(new Path(absSrcDir));

		for (FileStatus content : contents) {
			if (!content.isDir()) {
				if (ignoredFilesMatcher != null) {
					// Check for files to skip
					ignoredFilesMatcher.reset(content.getPath().toUri().getPath());
					if (ignoredFilesMatcher.matches()) {
						LOG.info("Ignoring " + content.getPath().toString());
						continue;
					}
				}
				files.add(new Text(content.getPath().toUri().getPath()));
			}
		}

		/*
		 * Is the directory empty?
		 */
		if (files.isEmpty()) {
			return;
		}

		/*
		 * We trick the reducer into doing some work for us by setting these configuration properties.
		 */
		job.set("mapred.tip.id",  "task_000000000000_00000_r_000000");
		job.set("mapred.task.id", "attempt_000000000000_0000_r_000000_0");

		job.set("mapred.output.dir", absOutDir);

		/*
		 * File output committer needs this.
		 */
		fs.mkdirs(new Path(absOutDir, "_temporary"));

		CrushReducer reducer = new CrushReducer();

		reducer.configure(job);
		reducer.reduce(bucket, files.iterator(), new NullOutputCollector<Text, Text>(), Reporter.NULL);		
		reducer.close();

		/*
		 * Use a glob here because the temporary and task attempt work dirs have funny names.
		 * Include a * at the end to cover wildcards for compressed files.
		 */
		Path crushOutput = new Path(absOutDir + "/*/*/crush" + absSrcDir + "/" + dest.getName() + "*");

		FileStatus[] statuses = fs.globStatus(crushOutput);

		if (statuses == null || 1 != statuses.length) {
			throw new AssertionError("Did not find the expected output in " + crushOutput.toString());
		}

		rename(statuses[0].getPath(), dest.getParent(), dest.getName());
	}

	private void cloneOutput() throws IOException {

		List<FileStatus> listStatus = getOutputMappings();

		/*
		 * Initialize to empty list, in which case swap() will be a no-op. The reference is then replaced with a real list, which is
		 * used in the subsequent iterations.
		 */
		List<Path> crushInput = emptyList();

		Text srcFile		= new Text();
		Text crushOut		= new Text();
		Text prevCrushOut	= new Text();

		for (FileStatus partFile : listStatus) {
			Path path = partFile.getPath();

			Reader reader = new Reader(fs, path, fs.getConf());

			try {
				while (reader.next(srcFile, crushOut)) {
					if (!crushOut.equals(prevCrushOut)) {
						swap(crushInput, prevCrushOut.toString());

						prevCrushOut.set(crushOut);
						crushInput = new LinkedList<Path>();
					}

					crushInput.add(new Path(srcFile.toString()));
				}
			} finally {
				try {
					reader.close();
				} catch (IOException e) {
					LOG.warn("Trapped exception when closing " + path, e);
				}
			}

			swap(crushInput, prevCrushOut.toString());
		}

		/*
		 * Don't forget to move the files that were not crushed to the output dir so that the output dir has all the data that was in
		 * the input dir, the difference being there are fewer files in the output dir.
		 */
		if (removableFiles.size() > 0) {
			String srcDirName = fs.makeQualified(srcDir).toUri().getPath();
			String destName = fs.makeQualified(dest).toUri().getPath();
			print(Verbosity.INFO, "\n\nMoving removed files to " + destName);
			for (String name : removableFiles) {
				Path srcPath = new Path(name);
				Path destPath = new Path(destName + name).getParent();
	
				print(Verbosity.INFO, "\n  Moving " + srcPath + " to " + destPath);
				rename(srcPath, destPath, null);
			}
		}
	}

	/**
	 * Returns the output from {@link CrushReducer}. Each reducer writes out a mapping of source files to crush output file.
	 */
	private List<FileStatus> getOutputMappings() throws IOException {
		try {
			FileStatus[] files = fs.listStatus(outDir, new PathFilter() {
				Matcher matcher = Pattern.compile("part-\\d+").matcher("dummy");

				@Override
				public boolean accept(Path path) {
					matcher.reset(path.getName());
	
					return matcher.matches();
				}
			});

			return asList(files);
		} catch (FileNotFoundException e) {
			return new LinkedList<FileStatus>();
		}
	}

	/**
	 * Moves the skipped files to the output directory. Called when operation in normal (non-clone) mode.
	 */
	private void moveOutput() throws IOException {

		List<FileStatus> listStatus = getOutputMappings();

		Text srcFile			= new Text();
		Text crushOut			= new Text();

		Set<String> crushOutputFiles = new HashSet<String>(nBuckets);

		for (FileStatus partFile : listStatus) {
			Path path = partFile.getPath();

			Reader reader = new Reader(fs, path, fs.getConf());

			try {
				while (reader.next(srcFile, crushOut)) {
					crushOutputFiles.add(new Path(crushOut.toString()).toUri().getPath());
				}
			} finally {
				try {
					reader.close();
				} catch (IOException e) {
					LOG.warn("Trapped exception when closing " + path, e);
				}
			}
		}

		assert crushOutputFiles.size() == nBuckets;

		/*
		 * The crushoutput files will appear in a subdirectory of the output directory. The subdirectory will be the full path of the
		 * input directory that was crushed. E.g.
		 *
		 * Crush input:
		 * /user/me/input/dir1/file1
		 * /user/me/input/dir1/file2
		 * /user/me/input/dir2/file3
		 * /user/me/input/dir2/file4
		 * /user/me/input/dir3/dir4/file5
		 * /user/me/input/dir3/dir4/file6
		 *
		 * Crush output:
		 * /user/me/output/user/me/input/dir1/crushed_file ...
		 * /user/me/output/user/me/input/dir2/crushed_file ...
		 * /user/me/output/user/me/input/dir2/dir3/dir4/crushed_file ...
		 *
		 * We need to collapse this down to:
		 * /user/me/output/dir1/crushed_file ...
		 * /user/me/output/dir2/crushed_file ...
		 * /user/me/output/dir2/dir3/dir4/crushed_file ...
		 */
		String srcDirName	= fs.makeQualified(srcDir).toUri().getPath();
		String destName		= fs.makeQualified(dest).toUri().getPath();
		String partToReplace	= fs.makeQualified(outDir).toUri().getPath() + "/crush" + srcDirName;

		for (String crushOutputFile : crushOutputFiles) {
			Path srcPath  = new Path(crushOutputFile);
			Path destPath = new Path(destName + crushOutputFile.substring(partToReplace.length())).getParent();

			print(Verbosity.INFO, "\n  Renaming " + srcPath + " to " + destPath);
			rename(srcPath, destPath, null);
		}

		/*
		 * Don't forget to move the files that were not crushed to the output dir so that the output dir has all the data that was in
		 * the input dir, the difference being there are fewer files in the output dir.
		 */
		if (skippedFiles.size() > 0) {
			print(Verbosity.INFO, "\n\nMoving skipped files to " + destName);

			for (String name : skippedFiles) {
				Path srcPath = new Path(name);
				Path destPath = new Path(destName + name.substring(srcDirName.length())).getParent();

				print(Verbosity.INFO, "\n  Renaming " + srcPath + " to " + destPath);
				rename(srcPath, destPath, null);
			}
		}
	}

	/**
	 * Moves all crush input files to {@link #dest} and then moves the crush output file to {@link #srcDir}.
	 */
	private void swap(List<Path> crushInput, String crushFileName) throws IOException {

		if (crushInput.isEmpty()) {
			return;
		}

		print(Verbosity.INFO, format("\n\nSwapping %s", crushFileName));

		List<Path> movedSrc  = new ArrayList<Path>(crushInput.size());
		List<Path> movedDest = new ArrayList<Path>(crushInput.size());

		Path crushedDir = crushInput.get(0).getParent();

		boolean crushFileNotInstalled = true;

		try {
			/*
			 * Move each source file into the clone directory, replacing the root with the path of the clone dir.
			 */
			for (Iterator<Path> iter = crushInput.iterator(); iter.hasNext(); ) {
				Path source = iter.next();

				/*
				 * Remove the leading slash from the input file to create a path relative to the clone dir.
				 */
				Path destPath = new Path(dest, source.toString().substring(1));

				rename(source, destPath.getParent(), null);

				iter.remove();

				movedSrc.add(source);
				movedDest.add(destPath);
			}


			/*
			 * Install the crush output file now that all the source files have been moved to the clone dir. Sometimes the compression
			 * codec messes with the names so watch out.
			 */
			Path crushFile = new Path(crushFileName);

			rename(crushFile, crushedDir, null);

			crushFileNotInstalled = false;

		} finally {
			if (!crushInput.isEmpty()) {
				/*
				 * We failed while moving the source files to the clone directory.
				 */
				LOG.error(format("Failed while moving files into the clone directory and before installing the crush output file (%d moved and %d remaining)",
						movedSrc.size(), crushInput.size()));

				StringBuilder sb = new StringBuilder("hadoop fs -mv ");

				for (int i = 0; i < movedSrc.size(); i++) {
					sb.append(" ");
					sb.append(movedDest.get(i));
				}

				sb.append(" ");
				sb.append(crushedDir);

				LOG.error("Execute the following to restore the file system to a good state: " + sb.toString());
			} else if (crushFileNotInstalled) {
				/*
				 * We failed moving the crush output file to the source directory.
				 */
				LOG.error(format("Failed while moving crush output file (%s) to the source directory (%s)", crushFileName, crushedDir));
			}
		}
	}

	/**
	 * Renames the source file to the destination file, taking into consideration that compression codes can mangle file names. Also
	 * ensures that the parent directory of the destination exists.
	 *
	 * @param src
	 *          The path to the file to copy.
	 * @param destDir
	 *          The dir to which the file must be copied
	 * @param fileName
	 *          The new name of the file or null to keep the original file name
	 *
	 * @throws IOException
	 */
	private void rename(Path src, Path destDir, String fileName) throws IOException {
		fs.mkdirs(destDir);

		if (null != codecExtension && !fs.exists(src)) {
			/*
			 * Try mangling the name like a codec would and invoke rename. Let execoptions bubble up.
			 */
			src = new Path(src + codecExtension);
		}

		Path dest;

		if (null == fileName) {
			dest = new Path(destDir, src.getName());
		} else {
			dest = new Path(destDir, fileName);
		}

		fs.rename(src, dest);
	}

	void writeDirs() throws IOException {

		print(Verbosity.INFO, "\nUsing temporary directory " + tmpDir.toUri().getPath() + "\n");

		FileStatus status = fs.getFileStatus(srcDir);

		Path tmpIn = new Path(tmpDir, "in");

		bucketFiles = new Path(tmpIn, "dirs");
		partitionMap = new Path(tmpIn, "partition-map");
		counters = new Path(tmpIn, "counters");

		skippedFiles = new HashSet<String>();
		removableFiles = new HashSet<String>();

		/*
		 * Prefer the path returned by the status because it is always fully qualified.
		 */
		List<Path> dirs = asList(status.getPath());

		Text key = new Text();
		Text value = new Text();

		Bucketer partitionBucketer = new Bucketer(maxTasks, 0, false);
		partitionBucketer.reset("partition-map");

		jobCounters = new Counters();
                int fileCount = 0;

		//Path bucketFile = new Path(tmpIn, "dirs_" + fileCount++);
		Writer writer = SequenceFile.createWriter(fs, job, bucketFiles, Text.class, Text.class, CompressionType.BLOCK);

		try {
			while (!dirs.isEmpty()) {
				List<Path> nextLevel = new LinkedList<Path>();

				for (Path dir : dirs) {
					String dirPath = dir.toUri().getPath();
					print(Verbosity.INFO, "\n\n[" + dirPath + "]");

					jobCounters.incrCounter(MapperCounter.DIRS_FOUND, 1);

					FileStatus[] contents = fs.listStatus(dir, new PathFilter() {
						@Override
						public boolean accept(Path testPath) {
							if (ignoredFilesMatcher == null) return true;
							ignoredFilesMatcher.reset(testPath.toUri().getPath());
							boolean ignores = ignoredFilesMatcher.matches();
							if (ignores)
								LOG.info("Ignoring file " + testPath);
							return !ignores;
						}
						
					});

					if (contents == null || contents.length == 0) {
						print(Verbosity.INFO, "\n  Directory is empty");

						jobCounters.incrCounter(MapperCounter.DIRS_SKIPPED, 1);
					} else {
						List<FileStatus> crushables = new ArrayList<FileStatus>(contents.length);
						Set<String> uncrushedFiles = new HashSet<String>(contents.length);

						long crushableBytes = 0;

						/*
						 * Queue sub directories for subsequent inspection and examine the files in this directory.
						 */
						for (FileStatus content : contents) {
							Path path = content.getPath();

							if (content.isDir()) {
								nextLevel.add(path);
							} else {
								String filePath = path.toUri().getPath();
								boolean skipFile = false;
								if (skippedFilesMatcher != null) {
									skippedFilesMatcher.reset(filePath);
									if (skippedFilesMatcher.matches()) {
										skipFile = true;
									}
								}

								boolean changed = uncrushedFiles.add(filePath);
								assert changed : path.toUri().getPath();
								long fileLength = content.getLen();

								if (!skipFile && fileLength <= maxEligibleSize) {
									if (removeEmptyFiles && fileLength == 0)
										removableFiles.add(filePath);
									else {
										crushables.add(content);
										crushableBytes += fileLength;
									}
								}
							}
						}

						/*
						 * We found a directory with data in it. Make sure we know how to name the crush output file and then increment the
						 * number of files we found.
						 */
						if (!uncrushedFiles.isEmpty()) {
							if (-1 == findMatcher(dir)) {
								throw new IllegalArgumentException("Could not find matching regex for directory: " + dir);
							}

							jobCounters.incrCounter(MapperCounter.FILES_FOUND, uncrushedFiles.size());
						}

						if (0 == crushableBytes) {
							print(Verbosity.INFO, "\n  Directory has no crushable files");

							jobCounters.incrCounter(MapperCounter.DIRS_SKIPPED, 1);
						} else {
							/*
							 * We found files to consider for crushing.
							 */
							long nBlocks = crushableBytes / dfsBlockSize;

							if (nBlocks * dfsBlockSize != crushableBytes) {
								nBlocks++;
							}

							/*
							 * maxFileBlocks will be huge in v1 mode, which will lead to one bucket per directory.
							 */
							long dirBuckets = nBlocks / maxFileBlocks;
							if (dirBuckets * maxFileBlocks != nBlocks) {
								dirBuckets++;
							}

							if (dirBuckets > Integer.MAX_VALUE) {
								throw new AssertionError("Too many buckets: " + dirBuckets);
							}

							Bucketer directoryBucketer = new Bucketer((int) dirBuckets, excludeSingleFileDirs);
							directoryBucketer.reset(getPathPart(dir));

							for (FileStatus file : crushables) {
								directoryBucketer.add(new FileStatusHasSize(file));
							}

							List<Bucket> crushFiles = directoryBucketer.createBuckets();
							if (crushFiles.isEmpty()) {
								jobCounters.incrCounter(MapperCounter.DIRS_SKIPPED, 1);
								print(Verbosity.INFO, "\n  Directory skipped");
							} else {
								nBuckets += crushFiles.size();
								jobCounters.incrCounter(MapperCounter.DIRS_ELIGIBLE, 1);
								print(Verbosity.INFO, "\n  Generating " + crushFiles.size() + " output files");

								/*
								 * Write out the mapping between a bucket and a file.
								 */
								for (Bucket crushFile : crushFiles) {
									String bucketId = crushFile.name();

									List<String> filesInBucket = crushFile.contents();

									print(Verbosity.INFO, format("\n  Output %s will include %,d input bytes from %,d files", bucketId,
										crushFile.size(), filesInBucket.size()));

									key.set(bucketId);

									for (String f : filesInBucket) {
										boolean changed = uncrushedFiles.remove(f);
										assert changed : f;

										pathMatcher.reset(f);
										pathMatcher.matches();

										value.set(pathMatcher.group(5));

										/*
										 * Write one row per file to maximize the number of mappers
										 */
										writer.append(key, value);

										/*
										 * Print the input file with four leading spaces.
										 */
										print(Verbosity.VERBOSE, "\n    " + f);
									}

									jobCounters.incrCounter(MapperCounter.FILES_ELIGIBLE, filesInBucket.size());

									partitionBucketer.add(crushFile);
								}
							}
						}

						if (!removableFiles.isEmpty()) {
							print(Verbosity.INFO, "\n  Marked " + removableFiles.size() + " files for removal");

							for (String removable : removableFiles) {
								uncrushedFiles.remove(removable);
								print(Verbosity.VERBOSE, "\n    " + removable);
							}

							jobCounters.incrCounter(MapperCounter.FILES_REMOVED, removableFiles.size());
						}

						if (!uncrushedFiles.isEmpty()) {
							print(Verbosity.INFO, "\n  Skipped " + uncrushedFiles.size() + " files");

							for (String uncrushed : uncrushedFiles) {
								print(Verbosity.VERBOSE, "\n    " + uncrushed);
							}

							jobCounters.incrCounter(MapperCounter.FILES_SKIPPED, uncrushedFiles.size());
						}

						skippedFiles.addAll(uncrushedFiles);
					}
				}

				dirs = nextLevel;
			}
		} finally {
			writer.close();
		}


		/*
		 * Now that we have processed all the directories, write the partition map.
		 */
		List<Bucket> partitions = partitionBucketer.createBuckets();
		assert partitions.size() <= maxTasks;

		writer = SequenceFile.createWriter(fs, job, partitionMap, Text.class, IntWritable.class);
		IntWritable partNum = new IntWritable();
                int totalReducers = 0;
		for (Bucket partition : partitions) {
			String partitionName = partition.name();

			int p = Integer.parseInt(partitionName.substring(partitionName.lastIndexOf('-') + 1));
			partNum.set(p);

			if (partition.contents().size() > 0)
				totalReducers++;

			for (String bucketId : partition.contents()) {
				key.set(bucketId);
				writer.append(key, partNum);
			}
		}
		writer.close();

                print(Verbosity.INFO, "\n\nNumber of allocated reducers = " + totalReducers);
		job.setInt("mapreduce.job.reduces", totalReducers);

		DataOutputStream countersStream = fs.create(this.counters);
		jobCounters.write(countersStream);
		countersStream.close();
	}

	/**
	 * Strips out the scheme and authority.
	 */
	private String getPathPart(Path path) {
		pathMatcher.reset(path.toString());

		pathMatcher.matches();

		return pathMatcher.group(5);
	}

	JobConf getJob() {
		return job;
	}

	FileSystem getFileSystem() {
		return fs;
	}

	void setFileSystem(FileSystem fs) {
		this.fs = fs;
	}

	Path getTmpDir() {
		return tmpDir;
	}

	Path getBucketFiles() {
		return bucketFiles;
	}

	Path getPartitionMap() {
		return partitionMap;
	}

	Path getCounters() {
		return counters;
	}

	public Counters getJobCounters() {
		return jobCounters;
	}

	int getMaxFileBlocks() {
		return maxFileBlocks;
	}

	private int findMatcher(Path path) {
		for (int i = 0; i < matchers.size(); i++) {
			Matcher matcher = matchers.get(i);

			matcher.reset(path.toUri().getPath());

			if (matcher.matches()) {
				return i;
			}
		}

		return -1;
	}

	private void print(Verbosity verbosity, String line) {
		if (verbosity.compareTo(console) >= 0) {
			out.print(line);
		}
	}

	private enum Verbosity {
		VERBOSE, INFO, NONE
	}

	public static void main(String[] args) throws Exception {

		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");

		Crush crusher = new Crush();

		int exitCode = ToolRunner.run(crusher, args);

		System.exit(exitCode);
	}

	private enum Mode {
		STAND_ALONE, MAP_REDUCE, CLONE
	}

	private static class NullOutputCollector<K, V> implements OutputCollector<K, V> {
		@Override
		public void collect(K arg0, V arg1) throws IOException {
		}
	}

	private static final Log LOG = LogFactory.getLog(Crush.class);
}
