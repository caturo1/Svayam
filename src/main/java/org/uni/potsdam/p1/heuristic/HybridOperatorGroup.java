package org.uni.potsdam.p1.heuristic;

import java.util.Map;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.uni.potsdam.p1.actors.operators.groups.AbstractOperatorGroup;
import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.EventOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;
import org.uni.potsdam.p1.types.outputTags.StringOutput;
import org.uni.potsdam.p1.variant.VariantSourceCounter;

/**
 * Operator group for hybrid load shedding setup.
 * Contains a single operator that handles its own statistics, analysis,
 * and communication with the coordinator.
 */
public class HybridOperatorGroup extends AbstractOperatorGroup {
    //Need side outputs for:
    // shedder -> thisOperator
    // thisOperator -> analyzer
    // thisOperator -> downstreamShedder
    // thisOperator -> downStreamAnalyzer

    public final MetricsOutput toThisAnalyser;
    public final StringOutput analyserInput;
    public final MetricsOutput downstreamAnalyser;
    public final EventOutput downstream;
    public StringOutput toKafka;
    public StringOutput kafkaToAnalyser;
    public StringOutput kafkaToOperator;
    
    // components of the operator group
    private HybridOperator operator;
    public HybridAnalyser analyser;
    public HybridMessenger messenger;

    public SingleOutputStreamOperator<String> messengerStream;
    public DataStream<Event> operatorStream;
    // upstream input from analyzer
    public DataStream<Metrics> analyzerInputStream;
    // upstream sos from analyzers
    public SingleOutputStreamOperator<String> analyserOutputStream; 
    // global KafkaProducer Stream
    public KafkaSink<String> globalChannelOut;
    public DataStream<String> analyserStringStream; 

    public HybridOperatorGroup(OperatorInfo operatorInfo) {
        super(operatorInfo);

        operator = new HybridOperator(operatorInfo);
        
        analyserInput = new StringOutput("to_analyser_" + operatorInfo.name);
        kafkaToAnalyser = new StringOutput("kafka_to_analyser_" + operatorInfo.name);
        kafkaToOperator = new StringOutput("kafka_to_operator_" + operatorInfo.name);
        toThisAnalyser = new MetricsOutput("op_to_analyser_" + operatorInfo.name);
        downstream = new EventOutput("downstream" + operatorInfo.name);  
        downstreamAnalyser = new MetricsOutput("downstream" + operatorInfo.name);  
        
        messenger = new HybridMessenger(operatorInfo, kafkaToAnalyser, kafkaToOperator);
        analyser = new HybridAnalyser(operatorInfo);
        
        // we only need outputRate and processingTimes for shedding calculations
        operator.setMetricsOutput("lambdaOut", toThisAnalyser);
        operator.setMetricsOutput("ptime", toThisAnalyser);
        operator.setMetricsOutput("lambdaOut", downstreamAnalyser);
    }

    /**
     * This should be the rough group architecture:
     *  input --> shedder --> operator --> output
     *              |           | 
     *              |        analyzer --> traversal
     *              |           |
     *                  ||
     *                Kafka        
     *  
     */
    public void createDataStream(DataStream<String> opKafkaStream) {
        String opName = operatorInfo.name;

        if (opKafkaStream == null) {
            throw new IllegalArgumentException("Kafka stream cannot be null for operator " + opName);
        }

        SingleOutputStreamOperator<String> messengerStream = opKafkaStream
            .process(messenger)
            .name("messenger_" + opName);

        DataStream<String> operatorKafkaStream = messengerStream
            .getSideOutput(kafkaToOperator);
        
        DataStream<String> analyzerKafkaStream = messengerStream
            .getSideOutput(kafkaToAnalyser);

        if (inputDataStreams == null) {
            throw new IllegalArgumentException("Input streams cannot be null for operator " + opName);
        }

        outputDataStream = inputDataStreams
            .connect(operatorKafkaStream)
            .process(operator)
            .name("operator_"+opName);
            
        DataStream<Metrics> operatorMetrics = outputDataStream.getSideOutput(toThisAnalyser);

        // Prepare the metrics stream for the analyzer
        DataStream<Metrics> metricsForAnalyzer;
        if (analyzerInputStream != null) {
            metricsForAnalyzer = analyzerInputStream.union(operatorMetrics);
        } else {
            metricsForAnalyzer = operatorMetrics;
        }
        
        if (analyserStringStream != null) {
            analyzerKafkaStream = analyzerKafkaStream.union(analyserStringStream);
        }

        analyserOutputStream = metricsForAnalyzer
            .connect(analyzerKafkaStream)
            .process(analyser)
            .name("analyser_" + opName);

        if (toKafka != null && globalChannelOut != null) {
            analyserOutputStream.getSideOutput(toKafka).sinkTo(globalChannelOut);
        }
    }

    @Override
    // Sets output for the operators
    public void setOutputs(Map<String, AbstractOperatorGroup> operatorGroupMap) {
        for (EventPattern pattern : operatorInfo.patterns) {
            for (String operatorName : pattern.downstreamOperators) {
                EventOutput currentComplexEventOutput = operatorGroupMap.get(operatorName).toThisOperator;
                operator.setSideOutput(operatorName, currentComplexEventOutput);
                HybridOperatorGroup dsOp = (HybridOperatorGroup) operatorGroupMap.get(operatorName);
                operator.setMetricsOutput("lambdaOut", dsOp.toThisAnalyser);
            }
        }
    }

    public void addInputStream(DataStream<Event> inputStream) {
        if (inputStream == null) {
            return;
        }

        if (this.inputDataStreams == null) {
            this.inputDataStreams = inputStream;
        } else {
            inputDataStreams = inputDataStreams.union(inputStream);
        }

        DataStream<Metrics> sourceOutputRates = inputStream
            .process(new VariantSourceCounter(operatorInfo))
            .name("Source_Counter_" + operatorInfo.name);

        if (analyzerInputStream == null) {
            analyzerInputStream = sourceOutputRates;
        } else {
            analyzerInputStream = analyzerInputStream.union(sourceOutputRates);
        }
    }

    // set metrics stream for inputRates to downstream analyzer
    public void addAnalyzerInputStream(SingleOutputStreamOperator<Event> inputRates) {
        if (inputRates == null) {
            return;
        }

        DataStream<Metrics> stream = inputRates.getSideOutput(toThisAnalyser);
        if (stream == null) {
            return;
        }

        if (analyzerInputStream == null) {
            analyzerInputStream = stream;
        } else {
            analyzerInputStream = analyzerInputStream.union(stream);
        }
    }

    // set sosStream for selectivities to downstream analyzer
    public void addAnalyserSOSStream(SingleOutputStreamOperator<String> sosStream) {
        if (sosStream == null) {
            return;
        }

        DataStream<String> incomingStream = sosStream.getSideOutput(analyserInput);

        if (analyserStringStream == null) {
            analyserStringStream = sosStream;
        } else {
            analyserStringStream = analyserStringStream.union(incomingStream);
        }
    }

    //set up the outgoing kafka connection the analyzer uses
    public HybridOperatorGroup connectToKafka(StringOutput produceToKafka,KafkaSink<String> globalChannelOut) {
        this.analyser.toKafka = produceToKafka;
        this.toKafka = produceToKafka;
        this.globalChannelOut = globalChannelOut;
        return this;
    }

}