package main.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class DBApp implements DBAppInterface {

    public DBApp() {
    }

    public void init() {
    }

    public void createTable(String tableName, String clusteringKey,
            Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        try {
            Table.exceptions(colNameType, colNameMin, colNameMax);

            File tableDirectory = new File("src/main/resources/data/");
            String[] pages = tableDirectory.list();
            boolean flag = false;
            if (pages.length == 0) {
                flag = false;
            } else {
                for (int i = 0; i < pages.length; i++) {
                    String pageName = pages[i];
                    if (pageName.equals(tableName + ".ser")) {
                        flag = true;
                        break;
                    }
                }
            }
            if (flag) {
                throw new DBAppException("The table already exists!");
            }

            // if (tableDirectory.exists())
            // throw new DBAppException("Already Exists");
            // else
            // tableDirectory.mkdir();

            Table tableInstance = new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax);

            try {
                FileOutputStream tableFile = new FileOutputStream("src/main/resources/data/" + tableName + ".ser");
                ObjectOutputStream out = new ObjectOutputStream(tableFile);
                out.writeObject(tableInstance);
                out.close();
                tableFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            wirteonMetaData(tableName, clusteringKey, colNameType, colNameMin, colNameMax);
        } catch (Exception e) {
            throw new DBAppException("Error in creating table");
        }

    }

    private void wirteonMetaData(String tableName, String clusteringKey,
            Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

        try {

            FileReader oldMetaDataFile = new FileReader("src/main/resources/metadata.csv");

            BufferedReader br = new BufferedReader(oldMetaDataFile);
            StringBuilder metaData = new StringBuilder();

            String curLine;
            while ((curLine = br.readLine()) != null)
                metaData.append(curLine).append('\n');
            FileWriter metaDataFile = new FileWriter("src/main/resources/metadata.csv");
            for (String colName : colNameType.keySet()) {
                metaData.append(tableName).append(",");
                metaData.append(colName).append(",");
                metaData.append(colNameType.get(colName)).append(",");
                metaData.append(colName.equals(clusteringKey) ? "True" : "False").append(",");
                metaData.append("null,");
                metaData.append("null,");
                metaData.append(colNameMin.get(colName)).append(",");
                metaData.append(colNameMax.get(colName));
                metaData.append("\n");
            }
            metaDataFile.write(metaData.toString());
            metaDataFile.close();
        } catch (Exception e) {
            throw new DBAppException("Error in writing on metadata");
        }
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue)
            throws DBAppException {
        try {
            insertMethods.insertIntoTable(tableName, colNameValue);
        } catch (Exception e) {
            throw new DBAppException("Error in insert");
        }
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue,
            Hashtable<String, Object> columnNameValue)
            throws DBAppException {
        try {
            updateMethods.updateTable(tableName, clusteringKeyValue, columnNameValue);
        } catch (Exception e) {
            throw new DBAppException("Error in update");
        }
    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        try {
            deleteFromMethods.deleteFromTable(tableName, columnNameValue);

        } catch (Exception e) {

            throw new DBAppException("Error in delete");
        }
    }

    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators)
            throws DBAppException {
        // TODO Auto-generated method stub
        try {
            Iterator<Hashtable<String, Object>> iterator = selectFromMethods.selectFromTable(sqlTerms, arrayOperators);

            return iterator;
        } catch (

        Exception e) {
            throw new DBAppException("Error in select");
        }

    }

    public void createIndex(String strTableName, String[] ColName)
            throws DBAppException {
        try {
            IndexMethods.createIndex(strTableName, ColName);
        } catch (Exception e) {
            throw new DBAppException("Error in create index");
        }
    }

    public static void main(String[] args) throws Exception {
        DBApp dbApp = new DBApp();
        // Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        // htblColNameType.put("id", "java.lang.Integer");
        // htblColNameType.put("name", "java.lang.String");
        // htblColNameType.put("date", "java.util.Date");
        // htblColNameType.put("gpa", "java.lang.Integer");

        // Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
        // Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
        // htblColNameMax.put("id", "10");
        // htblColNameMax.put("name", "Z");
        // htblColNameMax.put("gpa", "4");
        // htblColNameMax.put("date", "2021-01-01");
        // htblColNameMin.put("name", "A");
        // htblColNameMin.put("date", "2020-01-01");
        // htblColNameMin.put("gpa", "0");
        // htblColNameMin.put("id", "0");
        // dbApp.createTable("Teacher", "id", htblColNameType, htblColNameMin,
        // htblColNameMax);
        // /////////////////////////////

        // dbApp.createIndex("Teacher", new String[] { "id", "date", "gpa" });
        // createIndex("ahmed", new String[] { "ef", "dwl" });

        // insertions
        // Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
        // htblColNameValue.put("id", new Integer(9));
        // htblColNameValue.put("name", new String("M"));
        // htblColNameValue.put("gpa", new Integer(4));
        // htblColNameValue.put("date", new
        // SimpleDateFormat("yyyy-MM-dd").parse("2020-08-01"));
        // dbApp.insertIntoTable("Teacher", htblColNameValue);

        // selectinon
        // SQLTerm sqlTerm1 = new SQLTerm("Teacher", "id", "=", 1);
        // // SQLTerm sqlTerm2 = new SQLTerm("Teacher", "name", "=", "F");
        // // SQLTerm sqlTerm3 = new SQLTerm("Teacher", "gpa", "=", 2);
        // // SQLTerm sqlTerm4 = new SQLTerm("Teacher", "date", "=", new
        // // SimpleDateFormat("yyyy-MM-dd").parse("2020-04-01"));

        // SQLTerm[] sqlTerms = new SQLTerm[] { sqlTerm1
        // };
        // String[] arrayOperators = new String[] {};
        // Iterator<Hashtable<String, Object>> iterator =
        // dbApp.selectFromTable(sqlTerms, arrayOperators);
        // System.out.println(iterator.next());

        // // // <1,2,3> page 0
        // <4,5,6> page 1

        // FileInputStream fileIn = new
        // FileInputStream("src/main/resources/data/Teacheriddategpa.ser");
        // ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        // Node v = (Node) objectIn.readObject();
        // objectIn.close();
        // fileIn.close();

        // System.out.println(v.children.get(3).points.get(0).pageAndRow.get(0).clustringvalue);

        // // for (RowReference r : v.points) {
        // // System.out.println(r.pageAndRow.get(0).page);
        // // }

        // for (int i = 0; i < 8; i++) {
        // // Node Childs =
        // System.out.println(i);
        // Node number = v.children.get(i);
        // if (number.points.size() != 0)
        // System.out.println(number.points.get(0).pageAndRow.get(0).clustringvalue);
        // System.out.println("--------------------------");

        // }
        // // int number = v.children.get(2).children.get(6).points.size();

        // // System.out.println(number);
        // for (int i = 0; i < 8; i++) {
        // // Node Childs =
        // int number = v.children.get(i).points.size();
        // System.out.println(number);

        // }
        // for (Node child : ) {
        // System.out.println(child.children.size());

        // }

        // search

        // FileInputStream fileIn = new
        // FileInputStream("src/main/resources/data/Teacher.ser");
        // ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        // Table v = (Table) objectIn.readObject();
        // objectIn.close();
        // fileIn.close();
        // insertMethods.updateRange(v, "src/main/resources/data/Teacher0.ser");

        // System.out.println(v.getPages().get(0).getMaxClustering());
        // System.out.println(v.get(0).get("id"));

        // delete
        // Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
        // htblColNameValue.put("id", new Integer(9));
        // // htblColNameValue.put("name", new String("B"));
        // // htblColNameValue.put("gpa", new Integer(1));
        // //// htblColNameValue.put("date", new
        // //// SimpleDateFormat("yyyy-MM-dd").parse("2020-05-01"));

        // dbApp.deleteFromTable("Teacher", htblColNameValue);

        // when a record is deleted, values shift upwards leaving empty spaces in the
        // page from the bottom
        // value is re inserted in the correct place

        // update
        // Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
        // // htblColNameValue.put("name", new String("K"));
        // htblColNameValue.put("gpa", new Integer(1));
        // // htblColNameValue.put("date", new
        // // SimpleDateFormat("yyyy-MM-dd").parse("2020-06-01"));

        // dbApp.updateTable("Teacher", "9", htblColNameValue);

        // // //select
        // FileInputStream fileIn = new
        // FileInputStream("src/main/resources/data/Teacher0.ser");
        // ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        // Vector<Hashtable<String, Object>> v = (Vector<Hashtable<String, Object>>)
        // objectIn.readObject();
        // objectIn.close();
        // fileIn.close();

        // System.out.println(v.get(0));

        Bonus b = new Bonus();
        StringBuffer s = new StringBuffer("SELECT * FROM Teacher WHERE id=1 AND name=A");

        Iterator<Hashtable<String, Object>> iterator = Bonus.convertSql(s);
        System.out.println(iterator.next());

    }

}