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

import java.lang.Void;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;

//import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import parquet.schema.MessageType;
import parquet.schema.Type;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.column.ColumnDescriptor;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.file.SeekableInput;

@SuppressWarnings("deprecation")
public class CrushReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	private final Text valueOut = new Text();

	/**
	 * Internal counter for the number of input groups processed. Used to report status.
	 */
	private int fileNum;

	/**
	 * The number of source files that have been crushed.
	 */
	private int recordNumber;

	/**
	 * Report status when after processing this number of files.
	 */
	private int reportRecordNumber = 100;

	private int taskNum;

	private long timestamp;

	private JobConf job;

	private FileSystem fs;

	/**
	 * Matched against dir names to calculate the crush output file name.
	 */
	private List<Matcher> inputRegexList;

	/**
	 * Used with corresponding element in {@link #inputRegexList} to calculate the crush ouput file name.
	 */
	private List<String> outputReplacementList;

	/**
	 * Stores whether the input format is Text
	 */
	private String inputFormat;
	private String outputFormat;;
	private ParquetHiveSerDe parquetSerDe;

	/**
	 * Input formats that correspond with {@link #inputRegexList}.
	 */
	private List<Class<?>> inFormatClsList;

	/**
	 * Output formats that correspond with {@link #inputRegexList}.
	 */
	private List<Class<?>> outFormatClsList;

	/**
	 * Used to substitute values into placeholders.
	 */
	private Map<String, String> placeHolderToValue = new HashMap<String, String>(3);

	/**
	 * Used to locate placeholders in the replacement strings.
	 */
	private Matcher placeholderMatcher = Pattern.compile("\\$\\{([a-zA-Z]([a-zA-Z\\.]*))\\}").matcher("dummy");

	/**
	 * Path to the output dir of the job. Used to compute the final output file names for the crush files, which are the values in
	 * the reducer output.
	 */
	private String outDirPath;

	HashMap<String, String> decimalTypesHashMap = new HashMap();

	@Override
	public void configure(JobConf job) {
		super.configure(job);

		this.job = job;

		taskNum = Integer.parseInt(job.get("mapred.tip.id").replaceFirst(".+_(\\d+)", "$1"));
		timestamp = Long.parseLong(job.get("crush.timestamp"));

		outDirPath = job.get("mapred.output.dir");

		if (null == outDirPath || outDirPath.isEmpty()) {
			throw new IllegalArgumentException("mapred.output.dir has no value");
		}

		/*
		 * The files we write should be rooted in the "crush" subdir of the output directory to distinguish them from the files
		 * created by the collector.
		 */
		outDirPath = new Path(outDirPath + "/crush").toUri().getPath();

		/*
		 * Configure the regular expressions and replacements we use to convert dir names to crush output file names. Also get the
		 * directory data formats.
		 */
		int numSpecs = job.getInt("crush.num.specs", 0);

		if (numSpecs <= 0) {
			throw new IllegalArgumentException("Number of regular expressions must be zero or greater: " + numSpecs);
		}

		readCrushSpecs(numSpecs);

		placeHolderToValue.put("crush.task.num", Integer.toString(taskNum));
		placeHolderToValue.put("crush.timestamp", job.get("crush.timestamp"));

		try {
			fs = FileSystem.get(job);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Populates the following fields with non-default values from the configuration.
	 *
	 * <ul>
	 * <li><{@link #inputRegexList}/li>
	 * <li><{@link #outputReplacementList}/li>
	 * <li><{@link #inFormatClsList}/li>
	 * <li><{@link #outFormatClsList}/li>
	 * </ul>
	 */
	private void readCrushSpecs(int numSpecs) {
		inputRegexList = new ArrayList<Matcher>(numSpecs);
		outputReplacementList = new ArrayList<String>(numSpecs);
		inFormatClsList = new ArrayList<Class<?>>(numSpecs);
		outFormatClsList = new ArrayList<Class<?>>(numSpecs);

		for (int i = 0; i < numSpecs; i++) {
			String key;
			String value;

			/*
			 * Regex.
			 */
			key = format("crush.%d.regex", i);
			value = job.get(key);

			if (null == value || value.isEmpty()) {
				throw new IllegalArgumentException("No input regex: " + key);
			}

			inputRegexList.add(Pattern.compile(value).matcher("dummy"));

			/*
			 * Replacement for regex.
			 */
			key = format("crush.%d.regex.replacement", i);
			value = job.get(key);

			if (null == value || value.isEmpty()) {
				throw new IllegalArgumentException("No output replacement: " + key);
			}

			outputReplacementList.add(value);

			/*
			 * Input format
			 */
			key = format("crush.%d.input.format", i);
			value = job.get(key);

			if (null == value || value.isEmpty()) {
				throw new IllegalArgumentException("No input format: " + key);
			}

			try {
				Class<?> inFormatCls;

				if (value.equals(TextInputFormat.class.getName())) {
					inputFormat = "text";
					inFormatCls = KeyValuePreservingTextInputFormat.class;
				} else {
					inputFormat = value;
					inFormatCls = Class.forName(value);

					if (OrcInputFormat.class.isAssignableFrom(inFormatCls)) {
                        inputFormat = "orc";
                    }

                    if (!FileInputFormat.class.isAssignableFrom(inFormatCls)
                            && !OrcInputFormat.class.isAssignableFrom(inFormatCls)) {
                        throw new IllegalArgumentException(format("Not a file input format: %s=%s", key, value));
                    }
				}

				inFormatClsList.add(inFormatCls);
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException(format("Not a valid class: %s=%s", key, value));
			}

			/*
			 * Output format.
			 */
			key = format("crush.%d.output.format", i);
			value = job.get(key);

			if (null == value || value.isEmpty()) {
				throw new IllegalArgumentException("No output format: " + key);
			}

			try {
				Class<?> outFormatCls = Class.forName(value);

				if (!OutputFormat.class.isAssignableFrom(outFormatCls)) {
					throw new IllegalArgumentException(format("Not an output format: %s=%s", key, value));
				}

				outFormatClsList.add(outFormatCls);
			} catch (ClassNotFoundException e) {
				throw new IllegalArgumentException(format("Not a valid class: %s=%s", key, value));
			}
		}
	}

        private MessageType getParquetFileSchema(JobConf job, Path inputPath) throws IOException {
		ParquetMetadata readFooter = ParquetFileReader.readFooter(job, inputPath);
			return readFooter.getFileMetaData().getSchema();
        }

        private String getParquetFileSchemaString(JobConf job, Path inputPath) throws IOException {
		MessageType schema = getParquetFileSchema(job, inputPath);
		StringBuilder sc = new StringBuilder();
		schema.writeToStringBuilder(sc, "  ");
		return sc.toString();
        }

        private String getAvroFileSchemaString(JobConf job, Path inputPath) throws IOException {
		SeekableInput input = new FsInput(inputPath, job);
		DataFileReader<Void> avroReader = new DataFileReader<Void>(input, new GenericDatumReader<Void>());
		String schemaString = avroReader.getSchema().toString();
		avroReader.close();
                return schemaString;
        }
    private String getOrcFileSchemaString(JobConf job, Path inputPath) throws IOException {
        Reader reader = OrcFile.createReader(inputPath, new OrcFile.ReaderOptions(job));
        TypeDescription schema = reader.getSchema();
        String schemaString = schema.toString();
        return schemaString;
    }

    private ObjectInspector getOrcInspector(JobConf job, Path inputPath) throws IOException {
        org.apache.hadoop.hive.ql.io.orc.Reader reader = org.apache.hadoop.hive.ql.io.orc.OrcFile.createReader(FileSystem.get(job), inputPath);
        return reader.getObjectInspector();
    }

	@Override
	public void reduce(Text bucketId, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
		String bucket = bucketId.toString();

		String dirName = bucket.substring(0, bucket.lastIndexOf('-'));

		int idx = findMatcher(dirName);

		String outputFileName = calculateOutputFile(idx, dirName);

		/*
		 * Don't need to separate the paths because the output file name is already absolute.
		 */
		valueOut.set(outDirPath + outputFileName);

		LOG.info(format("Crushing bucket '%s' to file '%s'", bucket, outputFileName));

		/*
		 * Strip the leading slash to make the path relative. the output format will relativize it to the task attempt work dir.
		 */
		RecordWriter<Object, Object> sink = null;
		FileSinkOperator.RecordWriter parquetSink = null;
        RecordWriter orcWriter = null;
        OrcSerde orcSerde = new OrcSerde();
        Exception rootCause = null;

		Void voidKey = null;
		Object key = null;
		Object value = null;

		String schemaSignature = null;
		String columns = null;
		String columnsTypes = null;
		Properties jobProperties = new Properties();
		boolean firstFile = true;

		try {
			while (null == rootCause && values.hasNext()) {
				Text srcFile = values.next();
				Path inputPath = new Path(srcFile.toString());
                RecordReader<Object, Object> reader = null;
                reader = createRecordReader(idx, inputPath, reporter);

				if (firstFile) {
					firstFile = false;

					key = reader.createKey();
					if (null == key)
						key = NullWritable.get();
					value = reader.createValue();

					if (AvroContainerInputFormat.class.isAssignableFrom(getInputFormatClass(idx))) {
						schemaSignature = getAvroFileSchemaString(job, inputPath);
						job.set("avro.schema.literal", schemaSignature);
					} else if (OrcInputFormat.class.isAssignableFrom(getInputFormatClass(idx))) {
                        schemaSignature = getOrcFileSchemaString(job, inputPath);
                        // TODO
                    } else if (MapredParquetInputFormat.class.isAssignableFrom(getInputFormatClass(idx))) {
						MessageType schema = getParquetFileSchema(job, inputPath);
						List <Type> fieldsFromSchema = schema.getFields();
						for (Type field : fieldsFromSchema) {
							if (field.getOriginalType()!=null) {
								if (StringUtils.equals(field.getOriginalType().toString(), "DECIMAL")) {
									String primitiveType = field.asPrimitiveType().toString();
									int loc = primitiveType.indexOf("DECIMAL");
									int start = loc + 7;
									int end = primitiveType.indexOf(")", loc)+1;
									String ps = primitiveType.substring(start, end);
									if (!decimalTypesHashMap.containsKey(ps)){
										decimalTypesHashMap.put(field.getName().toString(),ps);
									}
								}
							}
						}
						schemaSignature = getParquetFileSchemaString(job, inputPath);
						StringBuilder columnsSb = new StringBuilder();
						StringBuilder columnsTypesSb = new StringBuilder();
						boolean firstColumn = true;
						for(ColumnDescriptor col : schema.getColumns()) {
							if (firstColumn) {
								firstColumn = false;
							} else {
								columnsSb.append(",");
								columnsTypesSb.append(",");
							}
							columnsSb.append(col.getPath()[0]);
							String typeName = col.getType().toString();
							if ("INT96".equals(typeName))
								typeName = "timestamp";
							else if ("INT64".equals(typeName))
								typeName = "bigint";
							else if ("INT32".equals(typeName))
								typeName = "int";
							else if ("INT16".equals(typeName))
								typeName = "smallint";
							else if ("INT8".equals(typeName))
								typeName = "tinyint";
							else if ("BINARY".equals(typeName))
								typeName = "string";
							else if ("BOOLEAN".equals(typeName))
								typeName = "boolean";
							else if ("DOUBLE".equals(typeName))
								typeName = "double";
							else if ("FLOAT".equals(typeName))
								typeName = "float";
							else if (typeName.startsWith("FIXED_LEN_BYTE_ARRAY")) {
								String column = col.toString();
								int start = column.indexOf('[') + 1;
								int end = column.indexOf(']');
								String fieldName = column.substring(start,end);
								String lookupVal = decimalTypesHashMap.get(fieldName);
								LOG.info("final string: decimal"+lookupVal);
								typeName="decimal"+lookupVal;
							}
							columnsTypesSb.append(typeName);
						}
						columns = columnsSb.toString();
						columnsTypes = columnsTypesSb.toString();
						jobProperties.put(IOConstants.COLUMNS, columns);
						jobProperties.put(IOConstants.COLUMNS_TYPES, columnsTypes);
						parquetSerDe = new ParquetHiveSerDe();
						parquetSerDe.initialize(job, jobProperties);
						}
						else {
						schemaSignature = key.getClass().getName() + ":" + value.getClass().getName();
						}


					/*
					 * Set the key and value class in the conf, which the output format uses to get type information.
					 */
					job.setOutputKeyClass(key.getClass());
					job.setOutputValueClass(value.getClass());

					/*
					 * Output file name is absolute so we can just add it to the crush prefix.
					 */
					if (MapredParquetOutputFormat.class.isAssignableFrom(getOutputFormatClass(idx))) {
						outputFormat = "parquet";
						parquetSink = createParquetRecordWriter(idx, valueOut.toString(), jobProperties, (Class<? extends org.apache.hadoop.io.Writable>)value.getClass(), reporter);
                    } else if (OrcOutputFormat.class.isAssignableFrom(getOutputFormatClass(idx))) {
                        outputFormat = "orc";
                        orcWriter = createOrcRecordWriter(idx, valueOut.toString(), reporter);
                    } else {
                        outputFormat = getOutputFormatClass(idx).getName();
                        sink = createRecordWriter(idx, valueOut.toString());
                    }
				} else { // next files

					/*
					 * Ensure schema signature is the same as the first file's
					 */
					String nextSchemaSignature = null;
					if (AvroContainerInputFormat.class.isAssignableFrom(getInputFormatClass(idx))) {
						nextSchemaSignature = getAvroFileSchemaString(job, inputPath);
                    } else if (OrcInputFormat.class.isAssignableFrom(getInputFormatClass(idx))) {
                        nextSchemaSignature = getOrcFileSchemaString(job, inputPath);
                    } else if (MapredParquetInputFormat.class.isAssignableFrom(getInputFormatClass(idx))) {
                        nextSchemaSignature = getParquetFileSchemaString(job, inputPath);
                    } else {
                        Object otherKey = reader.createKey();
                        if (otherKey == null)
                            otherKey = NullWritable.get();
                        nextSchemaSignature = otherKey.getClass().getName() + ":" + reader.createValue().getClass().getName();
                    }
					if (!schemaSignature.equals(nextSchemaSignature)) {
						throw new IllegalArgumentException(format("Heterogeneous schema detected in file %s: [%s] != [%s]", inputPath, nextSchemaSignature, schemaSignature));
					}
				}

				boolean ret;
				if ("parquet".equals(outputFormat)) {
                    ret = reader.next(voidKey, value);
                } else if ("orc".equals(outputFormat)) {
                    ret = reader.next(voidKey, value);
                } else {
                    ret = reader.next(key, value);
                }
				while (ret) {
					if ("text".equals(inputFormat)) {
                        sink.write(key, null);
                    } else if ("orc".equals(inputFormat)) {
                        orcWriter.write(NullWritable.get(), orcSerde.serialize(value, getOrcInspector(job, inputPath)));
                    } else {
                        if (sink != null)
                            sink.write(key, value);
                        else {
                            ParquetHiveRecord parquetHiveRecord = new ParquetHiveRecord(value, (StructObjectInspector) parquetSerDe.getObjectInspector());
                            parquetSink.write(parquetHiveRecord);
                        }
                    }
					reporter.incrCounter(ReducerCounter.RECORDS_CRUSHED, 1);

					if ("parquet".equals(outputFormat))
						ret = reader.next(voidKey, value);
                    else if ("orc".equals(outputFormat))
                        ret = reader.next(voidKey, value);
					else
						ret = reader.next(key, value);
				}
//
				/*
				 * Output of the reducer is the source file => crushed file (in the final output dir, no the task attempt work dir.
				 */
				collector.collect(srcFile, valueOut);
				reporter.incrCounter(ReducerCounter.FILES_CRUSHED, 1);

				recordNumber++;

				if (reportRecordNumber == recordNumber) {
					reportRecordNumber += reportRecordNumber;

					reporter.setStatus(format("Processed %,d files %s : %s", recordNumber, bucket, inputPath));
				}
			}
		} catch (Exception e) {
			rootCause = e;
		} finally {
			if (null != sink) {
				try {
					sink.close(reporter);
				} catch (Exception e) {
					if (null == rootCause) {
						rootCause = e;
					} else {
						LOG.error("Swallowing exception on close of " + outputFileName, e);
					}
				}
			}
			if (null != parquetSink) {
				try {
					parquetSink.close(false);
				} catch (Exception e) {
					if (null == rootCause) {
						rootCause = e;
					} else {
						LOG.error("Swallowing exception on close of " + outputFileName, e);
					}
				}
			}
            if (null != orcWriter) {
                try {
                    orcWriter.close(reporter);
                } catch (Exception e) {
                    if (null == rootCause) {
                        rootCause = e;
                    } else {
                        LOG.error("Swallowing exception on close of " + outputFileName, e);
                    }
                }
            }

			/*
			 * Let the exception bubble up with a minimum of wrapping.
			 */
			if (null != rootCause) {
				if (rootCause instanceof RuntimeException) {
					throw (RuntimeException) rootCause;
				}

				if (rootCause instanceof IOException) {
					throw (IOException) rootCause;
				}

				throw new RuntimeException(rootCause);
			}
		}
	}

        Class<? extends FileInputFormat<?, ?>> getInputFormatClass(int idx) {
                return (Class<? extends FileInputFormat<?, ?>>) inFormatClsList.get(idx);
        }

        Class<? extends OutputFormat<?, ?>> getOutputFormatClass(int idx) {
                return (Class<? extends OutputFormat<?, ?>>) outFormatClsList.get(idx);
        }

	/**
	 * Returns a record writer that creates files in the task attempt work directory. Path must be relative!
	 */
	@SuppressWarnings("unchecked")
	private RecordWriter<Object, Object> createRecordWriter(int idx, String path) throws IOException {
		Class<? extends OutputFormat<?, ?>> cls = (Class<? extends OutputFormat<?, ?>>) getOutputFormatClass(idx);

		try {
			OutputFormat<Object, Object> format = (OutputFormat<Object, Object>) cls.newInstance();
			return format.getRecordWriter(fs, job, path, null);
		} catch (RuntimeException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private RecordWriter createOrcRecordWriter(int idx, String path, Reporter reporter) throws IOException {
        Class<? extends OutputFormat<?, ?>> cls = (Class<? extends OutputFormat<?, ?>>) getOutputFormatClass(idx);
        try {
            OrcOutputFormat format = (OrcOutputFormat) cls.newInstance();
            return format.getRecordWriter(FileSystem.get(job),job,path.toString(), reporter);
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

	private FileSinkOperator.RecordWriter createParquetRecordWriter(int idx, String path, Properties properties, Class<? extends org.apache.hadoop.io.Writable> valueClass, Reporter reporter) throws IOException {
		Class<? extends OutputFormat<?, ?>> cls = (Class<? extends OutputFormat<?, ?>>) getOutputFormatClass(idx);

		try {
			MapredParquetOutputFormat format = (MapredParquetOutputFormat) cls.newInstance();
                       	return format.getHiveRecordWriter(job, new Path(path), valueClass, false /* isCompressed */, properties, reporter);
		} catch (RuntimeException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	private RecordReader<Object, Object> createRecordReader(int idx, Path inputPath, Reporter reporter) throws IOException {

		LOG.info(format("Opening '%s'", inputPath));

		Class<? extends FileInputFormat<?, ?>> cls = getInputFormatClass(idx);

		try {
			FileInputFormat.setInputPaths(job, inputPath);

			InputFormat<?, ?> instance = cls.newInstance();

			if (instance instanceof JobConfigurable) {
				((JobConfigurable) instance).configure(job);
			}

			InputSplit[] splits = instance.getSplits(job, 1);

			if (1 != splits.length) {
				throw new IllegalArgumentException("Could not get input splits: " + inputPath);
			}

			return (RecordReader<Object, Object>) instance.getRecordReader(splits[0], job, reporter);
		} catch (RuntimeException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Converts the name of a directory to a path to the crush output file using the specs at the given index. The path will the
	 * directory and file name separated by a slash /. Performs placeholder substitution on the corresponding replacement string in
	 * {@link #outputReplacementList}. The final replacement string is then used to form the final path.
	 */
	String calculateOutputFile(int idx, String srcDir) {

		StringBuffer sb = new StringBuffer(srcDir);
		sb.append("/");

		String replacement = outputReplacementList.get(idx);

		placeHolderToValue.put("crush.file.num", Integer.toString(fileNum++));

		placeholderMatcher.reset(replacement);

		while (placeholderMatcher.find()) {
			String key = placeholderMatcher.group(1);

			String value = placeHolderToValue.get(key);

			if (null == value) {
				throw new IllegalArgumentException("No value for key: " + key);
			}

			placeholderMatcher.appendReplacement(sb, value);
		}

		placeholderMatcher.appendTail(sb);

		Matcher matcher = inputRegexList.get(idx);
		matcher.reset(srcDir);

		String finalOutputName = matcher.replaceAll(sb.toString());

		return finalOutputName;
	}

	/**
	 * Returns the index into {@link #inputRegexList} of first pattern that matches the argument.
	 */
	int findMatcher(String dir) {

		String outputNameWithPlaceholders = null;

		for (int i = 0; i < inputRegexList.size() && outputNameWithPlaceholders == null; i++) {
			Matcher matcher = inputRegexList.get(i);

			matcher.reset(dir);

			if (matcher.matches()) {
				return i;
			}
		}

		throw new IllegalArgumentException("No matching input regex: " + dir);
	}

	int getTaskNum() {
		return taskNum;
	}

	long getTimestamp() {
		return timestamp;
	}

	List<String> getInputRegexList() {
		ArrayList<String> list = new ArrayList<String>(inputRegexList.size());

		for (Matcher matcher : inputRegexList) {
			list.add(matcher.pattern().pattern());
		}

		return list;
	}

	List<String> getOutputReplacementList() {
		return new ArrayList<String>(outputReplacementList);
	}

	List<Class<?>> getInputFormatList() {
		return new ArrayList<Class<?>>(inFormatClsList);
	}

	List<Class<?>> getOutputFormatList() {
		return new ArrayList<Class<?>>(outFormatClsList);
	}

	private static final Log LOG = LogFactory.getLog(CrushReducer.class);
}
