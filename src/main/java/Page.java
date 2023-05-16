package main.java;

import java.io.Serializable;

public class Page implements Serializable {

    private String path;
    private Object minClustering;
    private Object maxClustering;
    private int numberofRecords;
    private int maxNumberOfRecords;

    public Page(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Object getMinClustering() {
        return this.minClustering;
    }

    public void setMinClustering(Object minClusteringValue) {
        this.minClustering = minClusteringValue;
    }

    public Object getMaxClustering() {
        return this.maxClustering;
    }

    public void setMaxClustering(Object maxClusteringValue) {
        this.maxClustering = maxClusteringValue;
    }

    public int getNumberofRecords() {
        return this.numberofRecords;
    }

    public void setNumberofRecords(int numOfRecords) {
        this.numberofRecords = numOfRecords;
    }

    public int getMaxNumberOfRecords() {
        return this.maxNumberOfRecords;
    }

    public void setMaxNumberOfRecords(int maxNumberOfRecords) {
        this.maxNumberOfRecords = maxNumberOfRecords;
    }

}
