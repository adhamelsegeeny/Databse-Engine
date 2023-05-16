package main.java;

import java.io.Serializable;
import java.util.Vector;

class PageAndRow implements Serializable {
    int page;
    Object clustringvalue;

    PageAndRow(int p, Object clusteringObject) {
        page = p;
        this.clustringvalue = clusteringObject;
    }

}

public class RowReference implements Serializable {
    Vector<PageAndRow> pageAndRow;
    Object x;
    Object y;
    Object z;

    public RowReference(Object x, Object y, Object z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public Object getX() {
        return x;
    }

    public Object getY() {
        return y;
    }

    public Object getZ() {
        return z;
    }

}
