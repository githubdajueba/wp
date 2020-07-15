package cn.tedu.po;

public class Log {
    private String url;
    private String urlName;
    private String uvid;
    private String ssid;
    private String sscount;
    private String sstime;
    private String cip;
    private Long count;

    @Override
    public String toString() {
        return "Log{" +
                "url='" + url + '\'' +
                ", urlName='" + urlName + '\'' +
                ", uvid='" + uvid + '\'' +
                ", ssid='" + ssid + '\'' +
                ", sscount='" + sscount + '\'' +
                ", sstime='" + sstime + '\'' +
                ", cip='" + cip + '\'' +
                ", count=" + count +
                '}';
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrlName() {
        return urlName;
    }

    public void setUrlName(String urlName) {
        this.urlName = urlName;
    }

    public String getUvid() {
        return uvid;
    }

    public void setUvid(String uvid) {
        this.uvid = uvid;
    }

    public String getSsid() {
        return ssid;
    }

    public void setSsid(String ssid) {
        this.ssid = ssid;
    }

    public String getSscount() {
        return sscount;
    }

    public void setSscount(String sscount) {
        this.sscount = sscount;
    }

    public String getSstime() {
        return sstime;
    }

    public void setSstime(String sstime) {
        this.sstime = sstime;
    }

    public String getCip() {
        return cip;
    }

    public void setCip(String cip) {
        this.cip = cip;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
