package aiproject.utils;

public class XMLUtils {

    // 数据格式如下
    // <row Id="-1" Reputation="9" CreationDate="2010-07-28T16:38:27.683" DisplayName="Community" EmailHash="a007be5a61f6aa8f3e85ae2fc18dd66e" LastAccessDate="2010-07-28T16:38:27.683" Location="on the server farm" AboutMe="&lt;p&gt;Hi, I'm not really a person.&lt;/p&gt;&#xD;&#xA;&lt;p&gt;I'm a background process that helps keep this site clean!&lt;/p&gt;&#xD;&#xA;&lt;p&gt;I do things like&lt;/p&gt;&#xD;&#xA;&lt;ul&gt;&#xD;&#xA;&lt;li&gt;Randomly poke old unanswered questions every hour so they get some attention&lt;/li&gt;&#xD;&#xA;&lt;li&gt;Own community questions and answers so nobody gets unnecessary reputation from them&lt;/li&gt;&#xD;&#xA;&lt;li&gt;Own downvotes on spam/evil posts that get permanently deleted&#xD;&#xA;&lt;/ul&gt;" Views="0" UpVotes="142" DownVotes="119" />

    // 提取xml中对应的值
    public static String getAttrValInLine(String line, String attr) {
        String tmpAttr = " " + attr + "=\"";
        int start = line.indexOf(tmpAttr);
        if (start == -1) {
            return null;
        }
        start += tmpAttr.length();
        int end = line.indexOf("\"", start);
        return line.substring(start, end);
    }

    public static void main(String[] args) {
        String line = "<row Id=\"-1\" Reputation=\"9\" CreationDate=\"2010-07-28T16:38:27.683\" DisplayName=\"Community\" EmailHash=\"a007be5a61f6aa8f3e85ae2fc18dd66e\" LastAccessDate=\"2010-07-28T16:38:27.683\" Location=\"on the server farm\" AboutMe=\"&lt;p&gt;Hi, I'm not really a person.&lt;/p&gt;&#xD;&#xA;&lt;p&gt;I'm a background process that helps keep this site clean!&lt;/p&gt;&#xD;&#xA;&lt;p&gt;I do things like&lt;/p&gt;&#xD;&#xA;&lt;ul&gt;&#xD;&#xA;&lt;li&gt;Randomly poke old unanswered questions every hour so they get some attention&lt;/li&gt;&#xD;&#xA;&lt;li&gt;Own community questions and answers so nobody gets unnecessary reputation from them&lt;/li&gt;&#xD;&#xA;&lt;li&gt;Own downvotes on spam/evil posts that get permanently deleted&#xD;&#xA;&lt;/ul&gt;\" Views=\"0\" UpVotes=\"142\" DownVotes=\"119\" />";
        String attr = "EmailHash";
        String result = XMLUtils.getAttrValInLine(line, attr);
        System.out.println(result);
    }
}
