package main.java;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

import javax.print.attribute.HashAttributeSet;

public class Table implements Serializable {

    private String name;
    private String clusteringKey;
    private Vector<Hashtable<String, Object>> RangeNumber;
    private Hashtable<String, String> ColumnType;
    private Hashtable<String, String> ColumnMin;
    private Hashtable<String, String> ColumnMax;
    private Vector<Page> pages;
    Vector<Index> indexs;

    public Table(String tableName, String clustringKey, Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) {

        this.name = tableName;
        this.clusteringKey = clustringKey;
        this.ColumnType = colNameType;
        this.ColumnMin = colNameMin;
        this.ColumnMax = colNameMax;
        // this.pagesCounter = 0;
        this.pages = new Vector<Page>();
        this.RangeNumber = new Vector<Hashtable<String, Object>>();
        // this.maxCount = 0;

    }

    public void addIndex(Node root,
            String index1, String index2, String index3, String path) {
        indexs.add(new Index(root, index1, index2, index3, path));
    }

    @Override
    public String toString() {
        return "Table [tableName=" + name + ", clustringKey=" + clusteringKey +
                ", colNameType=" + ColumnType + ", colNameMin=" + ColumnMin + ", colNameMax=" + ColumnMax
                + ", minMaxCountN=" + RangeNumber + "]";
    }

    public static void exceptions(Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax)
            throws DBAppException {

        for (String columnKey : colNameMin.keySet())
            if (!(colNameType.containsKey(columnKey)))
                throw new DBAppException("Error");
        for (String columnKey : colNameMax.keySet())
            if (!(colNameType.containsKey(columnKey)))
                throw new DBAppException("Error");

    }

    public Page getPageByPath(String path) {
        for (Page p : pages)
            if (p.getPath().equals(path))
                return p;
        return null;
    }

    public String getTableName() {
        return name;
    }

    public Vector<Page> getPages() {
        return pages;
    }

    public String getName() {
        return name;
    }

    public String getClusteringKey() {
        return clusteringKey;
    }

    public Vector<Hashtable<String, Object>> getRangeNumber() {
        return RangeNumber;
    }

    public Hashtable<String, String> getColumnType() {
        return ColumnType;
    }

    public Hashtable<String, String> getColumnMin() {
        return ColumnMin;
    }

    public Hashtable<String, String> getColumnMax() {
        return ColumnMax;
    }

    public int getNumberofPages() {
        return pages.size();
    }

}
