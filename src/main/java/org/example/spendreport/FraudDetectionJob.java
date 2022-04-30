package org.example.spendreport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;


public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactionDataStream = env
                .addSource(new TransactionSource())
                .name("Transaction Source");

        DataStream<Alert> alertDataStream = transactionDataStream
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("Alert DataStream");

        alertDataStream.addSink(new AlertSink())
                .name("alert sink");

        env.execute("FraudDetection");
    }
}
