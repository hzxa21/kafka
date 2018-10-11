package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

public abstract class AbstractControlRequest extends AbstractRequest {
    public static final long UNKNOWN_BROKER_EPOCH = -1L;

    protected static final String CONTROLLER_ID_KEY_NAME = "controller_id";
    protected static final String CONTROLLER_EPOCH_KEY_NAME = "controller_epoch";
    protected static final String BROKER_EPOCH_KEY_NAME = "broker_epoch";


    protected final int controllerId;
    protected final int controllerEpoch;
    protected final long brokerEpoch;

    public static abstract class Builder<T extends AbstractRequest> extends AbstractRequest.Builder<T> {
        protected final int controllerId;
        protected final int controllerEpoch;
        protected final long brokerEpoch;

        protected Builder(ApiKeys api, short version, int controllerId, int controllerEpoch, long brokerEpoch) {
            super(api, version);
            this.controllerId = controllerId;
            this.controllerEpoch = controllerEpoch;
            this.brokerEpoch = brokerEpoch;
        }

    }

    public int controllerId() {
        return controllerId;
    }

    public int controllerEpoch() {
        return controllerEpoch;
    }

    public long brokerEpoch() { return brokerEpoch; }

    protected AbstractControlRequest(ApiKeys api, short version, int controllerId, int controllerEpoch, long brokerEpoch) {
        super(api, version);
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.brokerEpoch = brokerEpoch;
    }

    protected AbstractControlRequest(ApiKeys api, Struct struct, short version) {
        super(api, version);
        this.controllerId = struct.getInt(CONTROLLER_ID_KEY_NAME);
        this.controllerEpoch = struct.getInt(CONTROLLER_EPOCH_KEY_NAME);
        this.brokerEpoch = struct.hasField(BROKER_EPOCH_KEY_NAME) ? struct.getInt(BROKER_EPOCH_KEY_NAME) : UNKNOWN_BROKER_EPOCH;
    }


}
