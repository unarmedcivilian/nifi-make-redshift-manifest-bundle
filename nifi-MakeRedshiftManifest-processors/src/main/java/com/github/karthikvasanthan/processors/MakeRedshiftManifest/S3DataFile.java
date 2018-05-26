package com.github.karthikvasanthan.processors.MakeRedshiftManifest;

public class S3DataFile {

    public String url;
    public Boolean mandatory;

    public S3DataFile (String url, Boolean mandatory) {
        this.url = url;
        this.mandatory = mandatory;
    }

}
