package aiproject.entity;

import aiproject.utils.XMLUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// hadoop自定义序列化类型
public class User implements Writable {

    private String reputation;
    private String upVotes;
    private String downVotes;
    private String views;

    public User() {
    }

    public User(Text xmlLine){
        this.reputation = XMLUtils.getAttrValInLine(xmlLine.toString(), "Reputation");
        this.upVotes = XMLUtils.getAttrValInLine(xmlLine.toString(), "UpVotes");
        this.downVotes = XMLUtils.getAttrValInLine(xmlLine.toString(), "DownVotes");
        this.views = XMLUtils.getAttrValInLine(xmlLine.toString(), "Views");
    }

    public User(String displayName, String reputation, String upVotes, String downVotes, String views) {
        this.reputation = reputation;
        this.upVotes = upVotes;
        this.downVotes = downVotes;
        this.views = views;
    }

    public double[] getDoubleArr(){
        double[] arr = new double[4];
        arr[0] = Double.parseDouble(this.reputation);
        arr[1] = Double.parseDouble(this.upVotes);
        arr[2] = Double.parseDouble(this.downVotes);
        arr[3] = Double.parseDouble(this.views);
        return arr;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(reputation);
        dataOutput.writeChars(upVotes);
        dataOutput.writeChars(downVotes);
        dataOutput.writeChars(views);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.reputation = dataInput.readLine();
        this.upVotes = dataInput.readLine();
        this.downVotes = dataInput.readLine();
        this.views = dataInput.readLine();
    }

    public String getReputation() {
        return reputation;
    }

    public void setReputation(String reputation) {
        this.reputation = reputation;
    }

    public String getUpVotes() {
        return upVotes;
    }

    public void setUpVotes(String upVotes) {
        this.upVotes = upVotes;
    }

    public String getDownVotes() {
        return downVotes;
    }

    public void setDownVotes(String downVotes) {
        this.downVotes = downVotes;
    }

    public String getViews() {
        return views;
    }

    public void setViews(String views) {
        this.views = views;
    }

    @Override
    public String toString() {
        return reputation + "\t" + upVotes + "\t" + downVotes + "\t" + views;
    }
}
