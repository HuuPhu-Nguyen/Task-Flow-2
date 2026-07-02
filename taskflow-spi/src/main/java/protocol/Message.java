package protocol;

import com.google.gson.annotations.SerializedName;

import java.io.Serial;

public abstract class Message {
    @SerializedName("protocolVersion")
    int protocolVersion = ProtocolVersions.CURRENT;
    @SerializedName("type")
    String type;
    @SerializedName("nodeId")
    String nodeId;

    @SerializedName("time")
    String time;



    public String getNodeId() {
        return nodeId;
    }

    public String getType() {
        return type;
    }

    public String getTime() {
        return time;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }
}
