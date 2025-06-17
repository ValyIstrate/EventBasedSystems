package com.ebs.publisher.features.publisher.models;

import com.ebs.publisher.features.publisher.proto_classes.MessageProto;

public interface ProtobufConvertible {
    MessageProto.MsgProto toProto();
}