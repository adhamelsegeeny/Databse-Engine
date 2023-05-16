package main.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import javax.security.cert.X509Certificate;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class insertMethods {

    public static void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue)
            throws DBAppException, ClassNotFoundException, IOException, ParseException, Exception {
        String tablePath = "src/main/resources/data/" + tableName + ".ser";
        String pagePath;
        Table table = getTable(tablePath);
        String[] primaryKey = getPK(tableName);

        ckclusterisaval(tableName, colNameValue);
        checkInsertion(tableName, colNameValue);
        checkInsertion2(tableName, colNameValue);
        if (!checkCluster(tableName, colNameValue)) {
            throw new DBAppException("Already Exists");
        }
        int index = table.getPages().size();

        if (index == 0) {

            pagePath = createPage(table, colNameValue, primaryKey, index);
            if (returnIndex(tablePath, colNameValue) != -1) {
                insertIntoIndex(tableName, 0, colNameValue);
            }
        } else {
            int[] pageAndRecord = binarySearch(table, colNameValue);
            insertRowTarek(table, colNameValue, pageAndRecord[0], pageAndRecord[1]);
            if (returnIndex(tablePath, colNameValue) != -1) {
                insertIntoIndex(tableName, pageAndRecord[0], colNameValue);
            }

        }

    }

    private static void checkInsertion2(String tableName, Hashtable<String, Object> colNameValue)
            throws IOException, ParseException, DBAppException {
        Object[] tableInfo = updateMethods.getTableInfoMeta(tableName); // info about table
        Hashtable<String, String> colNames = (Hashtable<String, String>) tableInfo[0]; // column names
        Vector<String> colNamesVector = new Vector<String>();

        for (String key : colNames.keySet()) {
            colNamesVector.add(key);
        }
        for (String key : colNameValue.keySet()) {
            if (!colNamesVector.contains(key)) {

                throw new DBAppException("Column Name Not Found");
            }

        }
    }

    private static void ckclusterisaval(String tableName, Hashtable<String, Object> colNameValue)
            throws DBAppException, Exception {

        Object[] tableInfo = updateMethods.getTableInfoMeta(tableName); // info about table
        String clusteringCol = (String) tableInfo[4]; // clustering column name

        if (!colNameValue.containsKey(clusteringCol)) {
            throw new DBAppException("No Clustering Key");
        }

    }

    private static boolean checkCluster(String tableName, Hashtable<String, Object> colNameValue)
            throws ClassNotFoundException, IOException, ParseException, DBAppException {
        String path = "src/main/resources/data/" + tableName + ".ser";
        Table table = updateMethods.getTablefromCSV(path);
        Object[] tableMetaDataInfo = updateMethods.getTableInfoMeta(tableName);
        String clusteringColumn = (String) tableMetaDataInfo[4];
        Object clusteringObject = tableMetaDataInfo[1];

        int pageNumber = updateMethods.getPageTarek(table.getPages(), colNameValue.get(clusteringColumn));
        if (pageNumber == -1) {
            return true;

        }
        String pagePath = table.getPages().get(pageNumber).getPath();

        Vector<Hashtable<String, Object>> page = updateMethods.getPagesfromCSV(pagePath);
        int rowNumber = updateMethods.getRowTarek(page, clusteringColumn, colNameValue.get(clusteringColumn));

        if (rowNumber == -1) {
            return true;
        }
        return false;

    }

    public static int[] readConfig() {
        Properties prop = new Properties();
        String filePath = "src/main/resources/DBApp.config";
        InputStream is = null;
        try {
            is = new FileInputStream(filePath);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        try {
            prop.load(is);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        int[] arr = new int[2];
        arr[0] = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
        arr[1] = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));

        return arr;
    }

    private static int[] binarySearch(Table table, Hashtable<String, Object> colNameValue)
            throws ClassNotFoundException, IOException, ParseException, DBAppException {

        int[] pageAndRecord = new int[2];
        Object[] tableMetaDataInfo = updateMethods.getTableInfoMeta(table.getTableName());
        Object temp = tableMetaDataInfo[4];
        Object pk = colNameValue.get(temp);
        pageAndRecord[0] = updateMethods.getPage(table.getPages(), pk);
        String path = table.getPages().get(pageAndRecord[0]).getPath();
        Vector<Hashtable<String, Object>> records = updateMethods.getPagesfromCSV(path);
        pageAndRecord[1] = updateMethods.getRow(records, (String) tableMetaDataInfo[4], pk);
        return pageAndRecord;

    }

    public static void updateRange(Table table, String path)
            throws IOException, ParseException, DBAppException, ClassNotFoundException {
        Page page = table.getPageByPath(path);
        Object pk = updateMethods.getTableInfoMeta(table.getTableName())[4];
        Vector<Hashtable<String, Object>> vec = updateMethods.getPagesfromCSV(path);
        page.setMinClustering(vec.get(0).get(pk));
        page.setMaxClustering(vec.get(vec.size() - 1).get(pk));
        String tablePaths = "src/main/resources/data/" + table.getTableName() + ".ser";
        File f = new File(tablePaths);
        f.delete();

        FileOutputStream fileOut = new FileOutputStream(tablePaths);
        ObjectOutputStream objectOut = new ObjectOutputStream(
                fileOut);
        objectOut.writeObject(table);
        objectOut.close();
        fileOut.close();

    }

    private static void insertRowTarek(Table table, Hashtable<String, Object> colNameValue, int index, int recordIndex)
            throws IOException, ClassNotFoundException, ParseException, DBAppException {

        // deleteIntoIndex(table.getTableName(), index, recordIndex, colNameValue);
        String pagePath;

        if (index < table.getPages().size()) {
            pagePath = table.getPages().get(index).getPath();
        } else {
            String[] primaryKey = getPK(table.getTableName());
            checkInsertion(table.getTableName(), colNameValue);
            int i = table.getPages().size();
            createPage(table, colNameValue, primaryKey, i);
            return;

        }

        try {
            Vector<Hashtable<String, Object>> records = updateMethods.getPagesfromCSV(pagePath);
            records.insertElementAt(colNameValue, recordIndex);

            Page page = table.getPageByPath(pagePath);
            page.setNumberofRecords(page.getNumberofRecords() + 1);
            writeIntoDisk(table, "src/main/resources/data/" + table.getTableName() + ".ser");
            writeIntoDisk(records, pagePath);

            updateRange(table, pagePath);

            if (page.getNumberofRecords() > page.getMaxNumberOfRecords()) {
                Hashtable<String, Object> temp = records.get(records.size() - 1);

                records.remove(records.size() - 1);
                page = table.getPageByPath(pagePath);
                page.setNumberofRecords(page.getNumberofRecords() - 1);
                writeIntoDisk(table, "src/main/resources/data/" + table.getTableName() + ".ser");
                writeIntoDisk(records, pagePath);

                updateRange(table, pagePath);
                String tablePath = "src/main/resources/data/" + table.getTableName() + ".ser";
                Vector<Object> values = IndexMethods.columnIndexs(temp, tablePath);

                if (values != null) {

                    int number = insertMethods.returnIndex(tablePath, colNameValue);
                    Index indexIndex = table.indexs.get(number);
                    Node root = updateMethods
                            .getNodefromDisk(indexIndex.path);
                    root.updateRowrefrance(values.get(0), values.get(1), values.get(2),
                            index, temp.get(table.getClusteringKey()), index + 1);
                    deleteFromMethods.serialize(root, indexIndex.path);
                }

                insertRowTarek(table, temp, index + 1, 0);
            }
        } catch (Exception e) {

        }

    }

    public static int returnIndex(String path, Hashtable<String, Object> colName)
            throws ClassNotFoundException, IOException, ParseException, DBAppException {

        Table table = updateMethods.getTablefromCSV(path);
        if (table.indexs != null) {
            for (int i = 0; i < table.indexs.size(); i++) {
                Index index = table.indexs.get(i);
                if (index.index1 != null) {
                    if (colName.containsKey(index.index1) && colName.containsKey(index.index2)
                            && colName.containsKey(index.index3)) {

                        return i;
                    }
                }
            }
        }
        return -1;

    }

    static void insertIntoIndex(String tableName, int pageNumber,
            Hashtable<String, Object> colNameValue)
            throws ClassNotFoundException, IOException, ParseException, DBAppException {
        // .out.println("insert into index");

        String path;
        path = "src/main/resources/data/" + tableName + ".ser";
        Table table = updateMethods.getTablefromCSV(path);
        Vector<Object> values = IndexMethods.columnIndexs(colNameValue, path);
        if (values == null)
            return;

        Object clustringvalue = colNameValue.get(table.getClusteringKey());
        Object x = values.get(0);
        Object y = values.get(1);
        Object z = values.get(2);
        Index index = table.indexs.get(returnIndex(path, colNameValue));
        String indexPath = index.path;

        Node root = updateMethods.getNodefromDisk(indexPath);

        root.insert(clustringvalue, pageNumber, x, y, z);
        deleteFromMethods.serialize(root, indexPath);

    }

    static void writeIntoDisk(Object o, String path) throws IOException {

        File f = new File(path);
        f.delete();
        FileOutputStream tableRewrite = new FileOutputStream(path);
        ObjectOutputStream out = new ObjectOutputStream(tableRewrite);
        out.writeObject(o);
        out.close();
        tableRewrite.close();
    }

    private static void checkInsertion(String tableName, Hashtable<String, Object> colNameValue)
            throws DBAppException, ClassNotFoundException, IOException, ParseException {

        String path = "src/main/resources/metadata.csv";
        FileReader file = new FileReader(path);
        BufferedReader br = new BufferedReader(file);
        String currLine;
        while ((currLine = br.readLine()) != null) {
            String[] arr = currLine.split(",");
            if (arr[0].equals(tableName)) {
                if (!colNameValue.containsKey(arr[1])) {
                    colNameValue.put(arr[1], new NullWrapper());

                }
                if (arr[2].equals("java.lang.Integer")) {
                    if (!(colNameValue.get(arr[1]) instanceof Integer
                            || (colNameValue.get(arr[1]) instanceof NullWrapper))) {
                        throw new DBAppException("Error");
                    }
                    if (!(colNameValue.get(arr[1]) instanceof NullWrapper))
                        if ((Integer) colNameValue.get(arr[1]) < (Integer) Integer.parseInt(arr[6])
                                || (Integer) colNameValue.get(arr[1]) > (Integer) Integer.parseInt(arr[7])) {
                            throw new DBAppException("Error");
                        }

                } else if (arr[2].equals("java.lang.Double")) {

                    if (!(colNameValue.get(arr[1]) instanceof Double
                            || (colNameValue.get(arr[1]) instanceof NullWrapper))) {

                        throw new DBAppException("Error");
                    }
                    if (!(colNameValue.get(arr[1]) instanceof NullWrapper))
                        if ((Double) colNameValue.get(arr[1]) < (Double) Double.parseDouble(arr[6])
                                || (Double) colNameValue.get(arr[1]) > (Double) Double.parseDouble(arr[7])) {
                            throw new DBAppException("Error");
                        }

                } else if (arr[2].equals("java.lang.String")) {
                    if (!(colNameValue.get(arr[1]) instanceof String
                            || (colNameValue.get(arr[1]) instanceof NullWrapper))) {
                        throw new DBAppException("Error");
                    }
                    if (!(colNameValue.get(arr[1]) instanceof NullWrapper))
                        if (((String) colNameValue.get(arr[1])).compareTo((String) arr[6]) < 0) {
                            throw new DBAppException("Error");
                        }
                    if (!(colNameValue.get(arr[1]) instanceof NullWrapper))
                        if (((String) colNameValue.get(arr[1])).compareTo((String) arr[7]) > 0) {

                            throw new DBAppException("Error");
                        }

                } else if (arr[2].equals("java.util.Date")) {
                    if (!(colNameValue.get(arr[1]) instanceof Date
                            || (colNameValue.get(arr[1]) instanceof NullWrapper))) {
                        throw new DBAppException("Error");
                    } else {
                        if (!(colNameValue.get(arr[1]) instanceof NullWrapper))
                            if (((Date) colNameValue.get(arr[1]))
                                    .compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(arr[6])) < 0) {
                                throw new DBAppException("Error");
                            }

                            else if (((Date) colNameValue.get(arr[1]))
                                    .compareTo(new SimpleDateFormat("yyyy-MM-dd").parse(arr[7])) > 0) {
                                throw new DBAppException("Error");
                            }
                    }
                }
                if (arr[3].equals("True")) {
                    if (!colNameValue.containsKey(arr[1])) {
                        throw new DBAppException("Error");
                    }
                }

            }
        }

    }

    // get the type of the primary key and sets name
    private static String[] getPK(String tableName) {
        String path = "src/main/resources/metadata.csv";
        String[] pk = new String[2];
        try {
            FileReader file = new FileReader(path);
            BufferedReader br = new BufferedReader(file);
            String curLine;
            while ((curLine = br.readLine()) != null) {
                String[] arr = curLine.split(",");
                if (arr[0].equals(tableName) && arr[3].equals("True")) {
                    pk[0] = arr[1];
                    pk[1] = arr[2];
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return pk;
    }

    public static String createPage(Table table, Hashtable<String, Object> colNameValue, String[] primaryKey,
            int index) throws IOException {

        String path = "src/main/resources/data/" + table.getTableName() + index + ".ser";
        Page page = new Page(path);
        page.setMinClustering(colNameValue.get(primaryKey[0]));
        page.setMaxClustering(colNameValue.get(primaryKey[0]));
        table.getPages().add(page);
        page.setNumberofRecords(page.getNumberofRecords() + 1);
        page.setMaxNumberOfRecords(readConfig()[0]);
        Vector<Hashtable<String, Object>> record = new Vector<>();
        record.add(colNameValue);
        try {// serializing
            FileOutputStream pageFile = new FileOutputStream(page.getPath());
            ObjectOutputStream out = new ObjectOutputStream(pageFile);
            out.writeObject(record);
            out.close();
            pageFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {// serializing
            FileOutputStream tableFile = new FileOutputStream(
                    "src/main/resources/data/" + table.getTableName() + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(tableFile);
            out.writeObject(table);
            out.close();
            tableFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page.getPath();
    }

    public static Table getTable(String path) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(path);
        ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        Table table = (Table) objectIn.readObject();
        objectIn.close();
        fileIn.close();
        return table;

    }

}