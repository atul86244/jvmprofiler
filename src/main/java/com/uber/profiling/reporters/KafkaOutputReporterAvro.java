
package com.uber.profiling.reporters;
import com.jvmprofiler.*;
import com.uber.profiling.Reporter;
import com.uber.profiling.util.JsonUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.json.JSONArray;
import org.json.JSONObject;


public class KafkaOutputReporterAvro implements Reporter {
    private String brokerList = "localhost:9092";
    private boolean syncMode = false;

    private String topicPrefix;

    private ConcurrentHashMap<String, String> profilerTopics = new ConcurrentHashMap<>();

    //private Producer<String, JVMMetrics> producer;
    public KafkaOutputReporterAvro() {
    }

    public KafkaOutputReporterAvro(String brokerList, boolean syncMode, String topicPrefix) {
        this.brokerList = brokerList;
        this.syncMode = syncMode;
        this.topicPrefix = topicPrefix;
    }

    @Override
    public void report(String profilerName, Map<String, Object> metrics) throws Exception {


        String json = JsonUtils.serialize(metrics);

        String topicName = getTopic(profilerName);
        final JSONObject obj = new JSONObject(json);

        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("retries", 10);
        props.put("batch.size", 16384);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 16384000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://10.227.215.228:8081");
        if (syncMode) {
            props.put("acks", "all");
        }

        switch (profilerName) {

            case "CpuAndMemory":
                Producer<String, JVMMetrics> producer_CpuAndMemory = new KafkaProducer<String, JVMMetrics>(props);
                //ensureProducer(profilerName, producer_CpuAndMemory);
                final JSONArray memorypools_array = obj.getJSONArray("memoryPools");
                final JSONArray gc_array = obj.getJSONArray("gc");
                final JSONArray bufferpools_array = obj.getJSONArray("bufferPools");

                final int bufferpools_len = bufferpools_array.length();
                final int memorypools_len = memorypools_array.length();
                final int gc_len = gc_array.length();

                List<com.jvmprofiler.bufferPools> buffpools = new ArrayList<bufferPools>(bufferpools_len);

                for (int i = 0; i < bufferpools_len; ++i) {
                    final JSONObject bufferpool = bufferpools_array.getJSONObject(i);
                    bufferPools bufferpool_item = bufferPools.newBuilder()
                            .setTotalCapacity(bufferpool.getLong("totalCapacity"))
                            .setName(bufferpool.getString("name"))
                            .setCount(bufferpool.getLong("count"))
                            .setMemoryUsed(bufferpool.getLong("memoryUsed"))
                            .build();
                    buffpools.add(bufferpool_item);
                }

                List<com.jvmprofiler.memoryPools> mempools = new ArrayList<memoryPools>(memorypools_len);

                for (int i = 0; i < memorypools_len; ++i) {
                    final JSONObject memorypool = memorypools_array.getJSONObject(i);
                    memoryPools memorypool_item = memoryPools.newBuilder()
                            .setPeakUsageMax(memorypool.getLong("peakUsageMax"))
                            .setUsageMax(memorypool.getLong("usageMax"))
                            .setPeakUsageUsed(memorypool.getLong("peakUsageUsed"))
                            .setName(memorypool.getString("name"))
                            .setPeakUsageCommitted(memorypool.getLong("peakUsageCommitted"))
                            .setUsageUsed(memorypool.getLong("usageUsed"))
                            .setType(memorypool.getString("type"))
                            .setUsageCommitted(memorypool.getLong("usageCommitted"))
                            .build();
                    mempools.add(memorypool_item);
                }

                List<com.jvmprofiler.gc> gcs = new ArrayList<gc>(gc_len);

                for (int i = 0; i < gc_len; ++i) {
                    final JSONObject gc_json = gc_array.getJSONObject(i);
                    gc gc_item = gc.newBuilder()
                            .setCollectionTime(gc_json.getLong("collectionTime"))
                            .setName(gc_json.getString("name"))
                            .setCollectionCount(gc_json.getLong("collectionCount"))
                            .build();
                    gcs.add(gc_item);
                }

                JVMMetrics jvmmetrics = JVMMetrics.newBuilder()
                        .setNonHeapMemoryTotalUsed(obj.getDouble("nonHeapMemoryTotalUsed"))
                        .setBufferPools(buffpools)
                        .setHeapMemoryTotalUsed(obj.getDouble("heapMemoryTotalUsed"))
                        //.setVmRSS(obj.isNull("vmRSS") ? null : obj.getLong("vmRSS"))
                        .setEpochMillis(obj.getLong("epochMillis"))
                        .setNonHeapMemoryCommitted(obj.getDouble("nonHeapMemoryCommitted"))
                        .setHeapMemoryCommitted(obj.getDouble("heapMemoryCommitted"))
                        .setMemoryPools(mempools)
                        .setProcessCpuLoad(obj.getDouble("processCpuLoad"))
                        .setSystemCpuLoad(obj.getDouble("systemCpuLoad"))
                        .setProcessCpuTime(obj.getLong("processCpuTime"))
                        //.setVmHWM(obj.getLong("vmHWM"))
                        .setAppId(obj.getString("appId"))
                        .setName(obj.getString("name"))
                        .setHost(obj.getString("host"))
                        .setProcessUuid(obj.getString("processUuid"))
                        .setGc(gcs)
                        .build();

                Future<RecordMetadata> future1 = producer_CpuAndMemory.send(
                        new ProducerRecord<String, JVMMetrics>(topicName, jvmmetrics));
                if (syncMode) {
                    producer_CpuAndMemory.flush();
                    try {
                        future1.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                producer_CpuAndMemory.flush();
                producer_CpuAndMemory.close();
                break;

            case "MethodArgument":
                Producer<String, MethodArgument> producer_MethodArgument = new KafkaProducer<String, MethodArgument>(props);
               // ensureProducer(profilerName, producer_MethodArgument);
                MethodArgument methodargumentmetrics = MethodArgument.newBuilder()
                        .setMethodName(obj.getString("methodName"))
                        .setMetricName(obj.getString("metricName"))
                        .setClassName(obj.getString("className"))
                        .setMetricValue(obj.getLong("metricValue"))
                        .setProcessName(obj.getString("processName"))
                        .setEpochMillis(obj.getLong("epochMillis"))
                        .setAppId(obj.getString("appId"))
                        .setHost(obj.getString("host"))
                        .setProcessUuid(obj.getString("processUuid"))
                        .build();

                Future<RecordMetadata> future2 = producer_MethodArgument.send(
                        new ProducerRecord<String, MethodArgument>(topicName, methodargumentmetrics));
                if (syncMode) {
                    producer_MethodArgument.flush();
                    try {
                        future2.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                producer_MethodArgument.flush();
                producer_MethodArgument.close();
                break;

            case "MethodDuration":
                Producer<String, MethodDuration> producer_MethodDuration = new KafkaProducer<String, MethodDuration>(props);
                //ensureProducer(profilerName, producer_MethodDuration);
                MethodDuration methoddurationmetrics = MethodDuration.newBuilder()
                        .setMethodName(obj.getString("methodName"))
                        .setMetricName(obj.getString("metricName"))
                        .setClassName(obj.getString("className"))
                        .setMetricValue(obj.getLong("metricValue"))
                        .setProcessName(obj.getString("processName"))
                        .setEpochMillis(obj.getLong("epochMillis"))
                        .setAppId(obj.getString("appId"))
                        .setHost(obj.getString("host"))
                        .setProcessUuid(obj.getString("processUuid"))
                        .build();

                Future<RecordMetadata> future3 = producer_MethodDuration.send(
                        new ProducerRecord<String, MethodDuration>(topicName, methoddurationmetrics));
                if (syncMode) {
                    producer_MethodDuration.flush();
                    try {
                        future3.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                producer_MethodDuration.flush();
                producer_MethodDuration.close();
                break;

            case "ProcessInfo":
                Producer<String, ProcessInfo> producer_ProcessInfo = new KafkaProducer<String, ProcessInfo>(props);
                //ensureProducer(profilerName, producer_ProcessInfo);
                ProcessInfo processinfometrics = ProcessInfo.newBuilder()
                        .setJvmInputArguments(obj.getString("jvmInputArguments"))
                        .setCmdline(obj.getString("cmdline"))
                        .setJvmClassPath(obj.getString("jvmClassPath"))
                        .setAgentVersion(obj.getString("agentVersion"))
                        .setAppClass("null")
                        .setAppJar("null")
                        .setEpochMillis(obj.getLong("epochMillis"))
                        .setAppId(obj.getString("appId"))
                        .setName(obj.getString("name"))
                        .setHost(obj.getString("host"))
                        .setProcessUuid(obj.getString("processUuid"))
                        .build();

                Future<RecordMetadata> future4 = producer_ProcessInfo.send(
                        new ProducerRecord<String, ProcessInfo>(topicName, processinfometrics));
                if (syncMode) {
                    producer_ProcessInfo.flush();
                    try {
                        future4.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                producer_ProcessInfo.flush();
                producer_ProcessInfo.close();
                break;

            case "Stacktrace":
                Producer<String, Stacktrace> producer_Stacktrace = new KafkaProducer<String, Stacktrace>(props);
                //ensureProducer(profilerName, producer_Stacktrace);
                final JSONArray stacktrace_array = obj.getJSONArray("stacktrace");
                final int stacktrace_len = stacktrace_array.length();

                List<String> stacktraces = new ArrayList<String>(stacktrace_len);

                for (int i = 0; i < stacktrace_len; ++i) {
                    String item = stacktrace_array.getString(i);
                    stacktraces.add(item);
                }

                Stacktrace stacktracemetrics = Stacktrace.newBuilder()
                        .setStacktrace(stacktraces)
                        .setCount(obj.getLong("count"))
                        .setEndEpoch(obj.getLong("endEpoch"))
                        .setStartEpoch(obj.getLong("startEpoch"))
                        .setThreadName(obj.getString("threadName"))
                        .setThreadState(obj.getString("threadState"))
                        .setAppId(obj.getString("appId"))
                        .setName(obj.getString("name"))
                        .setHost(obj.getString("host"))
                        .setProcessUuid(obj.getString("processUuid"))
                        .build();

                Future<RecordMetadata> future5 = producer_Stacktrace.send(
                        new ProducerRecord<String, Stacktrace>(topicName, stacktracemetrics));
                if (syncMode) {
                    producer_Stacktrace.flush();
                    try {
                        future5.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                producer_Stacktrace.flush();
                producer_Stacktrace.close();
                break;
        }

    }

    @Override
    public void close() {
//        synchronized (this) {
//            if (producer == null) {
//                return;
//            }
//
//            producer.flush();
//            producer.close();
//
//            producer = null;
//        }
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public boolean isSyncMode() {
        return syncMode;
    }

    public void setSyncMode(boolean syncMode) {
        this.syncMode = syncMode;
    }

    public void setTopic(String profilerName, String topicName) {
        profilerTopics.put(profilerName, topicName);
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public String getTopic(String profilerName) {
        String topic = profilerTopics.getOrDefault(profilerName, null);
        if (topic == null || topic.isEmpty()) {
            topic = topicPrefix == null ? "" : topicPrefix;
            topic += profilerName;
        }
        return topic;
    }

}
