package main.java;

import java.io.Serializable;

public class Index implements Serializable{

    Node root;
    String index1 = null;
    String index2 = null;
    String index3 = null;

    String path = null;

    public Index(Node root, String x, String y, String z, String path) {

        this.root = root;
        this.index1 = x;
        this.index2 = y;
        this.index3 = z;
        this.path = path;
    }
}
