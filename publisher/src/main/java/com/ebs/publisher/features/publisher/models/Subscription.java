package com.ebs.publisher.features.publisher.models;

import com.ebs.publisher.features.publisher.proto_classes.SubscriptionProto;

import java.util.*;

public class Subscription {
    private Map<String, String> info = new LinkedHashMap<>();
    private List<String> operator = new ArrayList<>();

    public Subscription() {
    }

    public Map<String, String> getInfo() {
        return info;
    }

    public void addInfo(String metaInfo, String value) {
        this.info.put(metaInfo, value);
    }

    public void addOperator(String operator)
    {
        this.operator.add(operator);
    }

    public List<String> getOperator() {
        return operator;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Subscription{");

        int i = 0;
        for (Map.Entry<String, String> entry : info.entrySet()) {
            sb.append("(").append(entry.getKey()).append(",").append(operator.get(i)).append(",").append(entry.getValue()).append(");");
            i++;
        }

        sb.append("}\n");
        return sb.toString();
    }

    public SubscriptionProto.SubProto buildProto() {
        SubscriptionProto.SubProto.Builder proto = SubscriptionProto.SubProto.newBuilder();
        proto.addAllOperator(this.operator);
        proto.putAllInfo(this.info);
        return proto.build();
    }
}
