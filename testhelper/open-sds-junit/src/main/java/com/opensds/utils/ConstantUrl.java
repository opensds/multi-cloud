package com.opensds.utils;

public class ConstantUrl {
    private static ConstantUrl mConstantUrl;
    private static  String URL = null;
    private static  String PORT_TENANT_ID = null;
    private static  String PORT = null;

    private ConstantUrl() {
        PORT_TENANT_ID = getPortTenantId();
        PORT = getPort();
        URL = getHostIp();
    }

    public static ConstantUrl getInstance() {
        Logger.logString("**********************************************************************");
        if (mConstantUrl == null) {
            mConstantUrl = new ConstantUrl();
        }
        return mConstantUrl;
    }

    /**
     * Get port: This port used in there is used tenant id url
     */
    public String getPortTenantId() {
        return System.getenv("PORT_TENANT_ID");
    }

    /**
     * Get Port: This port used in S3 services url except login or auth related url.
     */
    public String getPort() {
        return System.getenv("PORT");
    }

    /**
     * Get Host Ip.
     */
    public String getHostIp() {
        return "http://" + System.getenv("HOST_IP");
    }

    /**
     * Get Token Login.
     */
    public String getTokenLogin() {
        return URL +"/identity/v3/auth/tokens";
    }

    /**
     * Get aks list.
     *
     * @param userId user id.
     */
    public String getAksList(String userId) {
        return URL +"/identity/v3/credentials?userId="+userId+"&type=ec2";
    }

    /**
     * Get Types
     *
     * @param tenantId tenant id.
     */
    public String getTypesUrl(String tenantId) {
        return URL+PORT_TENANT_ID+"/"+tenantId+"/types";
    }

    /**
     * Add Backend
     *
     * @param tenantId admin tenant id.
     */
    public String getAddBackendUrl(String tenantId) {
        return URL+PORT_TENANT_ID+"/"+ tenantId +"/backends";
    }

    /**
     * Create Bucket
     *
     * @param bucketName bucket name.
     */
    public String getCreateBucketUrl(String bucketName) {
        return URL+PORT+"/"+ bucketName;
    }

    /**
     * Bucket List
     */
    public String getListBucketUrl() {
        return URL+PORT+"/";
    }
}
