package main.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Vector;

public class updateMethods {

    static void updateTable(String tableName, String clusteringKeyValue,
            Hashtable<String, Object> columnNameValue)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {
        String path = "src/main/resources/data/" + tableName + ".ser";
        Table table = getTablefromCSV(path);
        Object[] tableMetaDataInfo = getTableInfoMeta(tableName);
        String clusteringColumn = (String) tableMetaDataInfo[4];
        Object clusteringObject = tableMetaDataInfo[1];
        int pageNumber;
        Hashtable<String, Object> forIndex = null;
        pageNumber = getPageTarek(table.getPages(), clusteringKeyValue);
        if (pageNumber == -1) {
            throw new DBAppException("error");

        }
        String pagePath = table.getPages().get(pageNumber).getPath();

        Vector<Hashtable<String, Object>> page = getPagesfromCSV(pagePath);
        int rowNumber = getRowTarek(page, clusteringColumn, clusteringKeyValue);

        if (rowNumber == -1) {
            throw new DBAppException("error");
        }
        forIndex = page.get(rowNumber);
        Hashtable<String, Object> forIndex2 = null;

        for (String key : columnNameValue.keySet()) {

            page.get(rowNumber).replace(key, columnNameValue.get(key));
        }
        forIndex2 = page.get(rowNumber);
        File f = new File(table.getPages().get(pageNumber).getPath());
        f.delete();
        String path2 = table.getPages().get(pageNumber).getPath();
        FileOutputStream fileOut = new FileOutputStream(path2);
        ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
        objectOut.writeObject(page);
        objectOut.close();
        fileOut.close();
        int number = insertMethods.returnIndex("src/main/resources/data/" + table.getTableName() + ".ser",
                forIndex);
        if (forIndex != null && number != -1) {

            // System.out.println("herrrrr");
            // Index index = table.indexs.get(number);
            // Node root = updateMethods.getNodefromDisk(index.path);
            // Vector<Object> v = IndexMethods.columnIndexs(forIndex, path);

            // root.deleteRowrefrance(v.get(0), v.get(1), v.get(2), pageNumber,
            // forIndex.get(table.getClusteringKey()));
            // deleteFromMethods.serialize(root, index.path);
            // // System.out.println(forIndex2);
            // insertMethods.insertIntoIndex(tableName, pageNumber, forIndex2);
            // deleteFromMethods.serialize(root, index.path);
            IndexMethods.updateIndex(tableName);

        }

    }

    public static int checkTarek(Object clusteringObject1, Object clusteringObject2) {

        if (clusteringObject2 instanceof java.lang.Integer) {

            String s = clusteringObject1.toString();
            Integer x = Integer.parseInt(s);
            Integer y = (Integer) clusteringObject2;
            return (x.compareTo(y));
        } else if (clusteringObject2 instanceof java.lang.Double) {
            String s = clusteringObject1.toString();
            Double x = Double.parseDouble(s);
            Double y = (Double) clusteringObject2;
            return (x.compareTo(y));
        } else if (clusteringObject2 instanceof java.util.Date) {
            return ((Date) clusteringObject1).compareTo((Date) clusteringObject2);
        } else {
            return ((String) clusteringObject1).compareTo((String) clusteringObject2);
        }
    }

    public static int getRowTarek(Vector<Hashtable<String, Object>> page, String clusteringColumn,
            Object clusteringObject) {
        int low = 0;
        int high = page.size() - 1;
        int mid = (low + high) / 2;
        while (low <= high) {
            mid = (low + high) / 2;
            Object object2 = page.get(mid).get(clusteringColumn);
            if (checkTarek(clusteringObject, object2) == 0) {
                return mid;
            } else if (checkTarek(clusteringObject, object2) < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return -1;

    }

    public static int getRow(Vector<Hashtable<String, Object>> page, String clusteringColumn,
            Object clusteringObject) {
        int low = 0;
        int high = page.size() - 1;
        int mid = (low + high) / 2;
        while (low <= high) {
            mid = (low + high) / 2;
            Object object2 = page.get(mid).get(clusteringColumn);

            if (check(clusteringObject, object2) == 0) {
                return mid;
            } else if (check(clusteringObject, object2) < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return low;

    }

    public static Vector<Hashtable<String, Object>> getPagesfromCSV(String path)
            throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(path);
        ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        Object pages = objectIn.readObject();
        objectIn.close();
        fileIn.close();
        return (Vector<Hashtable<String, Object>>) pages;
    }

    public static int getPageTarek(Vector<Page> pages, Object clusteringObject) {
        int low = 0;
        int high = pages.size() - 1;
        int mid = (low + high) / 2;
        while (low <= high) {
            mid = (low + high) / 2;
            Object lowCompare = pages.get(mid).getMinClustering();
            Object highCompare = pages.get(mid).getMaxClustering();

            if (checkTarek(clusteringObject, highCompare) <= 0 && checkTarek(clusteringObject, lowCompare) >= 0) {

                return mid;
            } else if (checkTarek(clusteringObject, lowCompare) < 0) {

                high = mid - 1;

            }

            else
                low = mid + 1;

        }

        return -1;

    }

    public static int getPage(Vector<Page> pages, Object clusteringObject) {
        int low = 0;
        int high = pages.size() - 1;
        int mid = (low + high) / 2;
        while (low <= high) {
            mid = (low + high) / 2;
            Object lowCompare = pages.get(mid).getMinClustering();
            Object highCompare = pages.get(mid).getMaxClustering();

            if (check(clusteringObject, highCompare) <= 0 && check(clusteringObject, lowCompare) >= 0)
                return mid;
            else if (check(clusteringObject, lowCompare) <= 0)
                high = mid - 1;
            else
                low = mid + 1;
        }
        if (pages.size() <= low) {
            return pages.size() - 1;
        } else {
            return low;
        }
    }

    public static int check(Object clusteringObject1, Object clusteringObject2) {
        if (clusteringObject1 instanceof java.lang.Integer) {
            return ((Integer) clusteringObject1).compareTo((Integer) clusteringObject2);
        } else if (clusteringObject1 instanceof java.lang.Double) {
            return ((Double) clusteringObject1).compareTo((Double) clusteringObject2);
        } else if (clusteringObject1 instanceof java.util.Date) {
            return ((Date) clusteringObject1).compareTo((Date) clusteringObject2);
        } else {
            return ((String) clusteringObject1).compareTo((String) clusteringObject2);
        }
    }

    public static Table getTablefromCSV(String path) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(path);
        ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        Object table = objectIn.readObject();
        objectIn.close();
        fileIn.close();
        return (Table) table;
    }

    public static Node getNodefromDisk(String path) throws IOException, ClassNotFoundException {
        // .out.println(path);
        FileInputStream fileIn = new FileInputStream(path);

        ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        Object node = objectIn.readObject();
        objectIn.close();
        fileIn.close();
        return (Node) node;
    }

    public static Object[] getTableInfoMeta(String tableName) throws IOException, ParseException, DBAppException {
        FileReader metadata = new FileReader("src/main/resources/metadata.csv");
        BufferedReader br = new BufferedReader(metadata);

        Hashtable<String, String> colDataType = new Hashtable<>();
        Hashtable<String, Object> columnMin = new Hashtable<>();
        Hashtable<String, Object> columnMax = new Hashtable<>();
        String clusteringType = "", clusteringCol = "";
        String curLine;
        while ((curLine = br.readLine()) != null) {
            String[] curLineSplit = curLine.split(",");
            if (curLineSplit[0].equals(tableName)) {
                colDataType.put(curLineSplit[1], curLineSplit[2]);
                switch (curLineSplit[2]) {
                    case "java.lang.Integer":
                        columnMin.put(curLineSplit[1], Integer.parseInt(curLineSplit[6]));
                        columnMax.put(curLineSplit[1], Integer.parseInt(curLineSplit[7]));
                        break;
                    case "java.lang.Double":
                        columnMin.put(curLineSplit[1], Double.parseDouble(curLineSplit[6]));
                        columnMax.put(curLineSplit[1], Double.parseDouble(curLineSplit[7]));
                        break;
                    case "java.util.Date":
                        columnMin.put(curLineSplit[1], new SimpleDateFormat("yyyy-MM-dd").parse(curLineSplit[6]));
                        columnMax.put(curLineSplit[1], new SimpleDateFormat("yyyy-MM-dd").parse(curLineSplit[7]));
                        break;
                    default:
                        columnMin.put(curLineSplit[1], curLineSplit[6]);
                        columnMax.put(curLineSplit[1], curLineSplit[7]);
                        break;
                }
                if (curLineSplit[3].equals("True")) {
                    clusteringType = curLineSplit[2];
                    clusteringCol = curLineSplit[1];
                }
            }
        }
        return new Object[] { colDataType, columnMin, columnMax, clusteringType, clusteringCol };

    }

}