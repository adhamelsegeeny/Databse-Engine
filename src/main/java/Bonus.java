package main.java;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class Bonus {

    public static Iterator callSelect(String table,
            Vector<String> conditions, Vector<String> logicalOperators)
            throws ClassNotFoundException, IOException, ParseException, DBAppException {

        String path = "src/main/resources/data/" + table + ".ser";
        // Table t= updateMethods.getTablefromCSV(path);
        Object[] meta = updateMethods.getTableInfoMeta(table);
        String[] cond = new String[2];
        SQLTerm[] sqlTerms = new SQLTerm[conditions.size()];
        String[] operators = new String[logicalOperators.size()];

        System.out.println(table);
        System.out.println(conditions);
        System.out.println(logicalOperators);

        for (int i = 0; i < operators.length; i++) {
            operators[i] = logicalOperators.get(i);
        }

        for (int i = 0; i < conditions.size(); i++) {
            String termCol = "";
            String currOper = "";
            Object currVal = null;

            // System.out.println(conditions.get(i).length());
            for (int j = 0; j < conditions.get(i).length(); j++) {
                // System.out.println((conditions.get(0).charAt(2) == '=') + "bhu");
                if ((conditions.get(i).charAt(j) == '=')) {

                    cond = conditions.get(i).split("=");

                    currOper = "=";
                    break;
                }

                else if (conditions.get(i).charAt(j) == '<' && conditions.get(i).charAt(j + 1) == '=') {
                    cond = conditions.get(i).split("<=");

                    currOper = "<=";
                    break;
                } else if (conditions.get(i).charAt(j) == '>' && conditions.get(i).charAt(j + 1) == '=') {
                    cond = conditions.get(i).split(">=");

                    currOper = ">=";
                    break;
                } else if (conditions.get(i).charAt(j) == '<' && conditions.get(i).charAt(j + 1) == '>') {
                    cond = conditions.get(i).split("<>");

                    currOper = "<>";
                    break;
                } else if (conditions.get(i).charAt(j) == '<') {
                    cond = conditions.get(i).split("<");

                    currOper = "<";
                    break;
                } else if (conditions.get(i).charAt(j) == '>') {
                    cond = conditions.get(i).split(">");

                    currOper = ">";
                    break;
                }

            }
            Hashtable<String, String> temp = (Hashtable<String, String>) meta[0];
            // Hashtable<String, Object> currentCond = new Hashtable<>();
            System.out.println(cond[0]);
            String type = temp.get(cond[0]);
            if (type.equals("java.lang.Integer")) {
                termCol = cond[0];
                currVal = Integer.parseInt(cond[1]);

            } else if (type.equals("java.lang.Double")) {
                termCol = cond[0];
                currVal = Double.parseDouble(cond[1]);

            } else if (type.equals("java.lang.Date")) {
                termCol = cond[0];
                currVal = new SimpleDateFormat("yyyy-MM-dd").parse(cond[1]);

            } else {// string
                termCol = cond[0];
                currVal = cond[1];

            }

            sqlTerms[i] = new SQLTerm(table, termCol, currOper, currVal);

        }

        return selectFromMethods.selectFromTable(sqlTerms, operators);

    }

    public static void callCreateIndex(String table, String[] columns)
            throws ClassNotFoundException, DBAppException, IOException, ParseException {

        System.out.println(table);
        System.out.println(columns[0]);

        IndexMethods.createIndex(table, columns);
    }

    public static void callDelete(String table, Vector<String> conditions, Vector<String> logicalOperators)
            throws Exception {

        Object[] meta = updateMethods.getTableInfoMeta(table);
        String[] cond = new String[2];
        String[] operators = new String[logicalOperators.size()];
        Hashtable<String, Object> deleteValues = new Hashtable<>();
        String[] values = null;
        for (int i = 0; i < operators.length; i++) {
            operators[i] = logicalOperators.get(i);
        }

        for (int i = 0; i < conditions.size(); i++) {
            cond = conditions.get(i).split("=");

            Hashtable<String, String> temp = (Hashtable<String, String>) meta[0];
            Hashtable<String, Object> currentCond = new Hashtable<>();
            String type = temp.get(cond[0]);

            System.out.println(table);
            System.out.println(conditions);
            System.out.println(logicalOperators);

            if (type.equals("java.lang.Integer")) {

                deleteValues.put(cond[0], Integer.parseInt(cond[1]));

            } else if (type.equals("java.lang.Double")) {

                deleteValues.put(cond[0], Double.parseDouble(cond[1]));

            } else if (type.equals("java.lang.Date")) {
                deleteValues.put(cond[0], new SimpleDateFormat("yyyy-MM-dd").parse(cond[1]));

            } else {// string
                deleteValues.put(cond[0], cond[1]);

            }
        }

        deleteFromMethods.deleteFromTable(table, deleteValues);
    }

    public static void callUpdate(String table, String clusterValue, String[] updates)
            throws Exception {

        Object[] meta = updateMethods.getTableInfoMeta(table);
        String[] cond = new String[2];
        Hashtable<String, Object> updateValues = new Hashtable<>();

        System.out.println(table);
        System.out.println(clusterValue);
        System.out.println(updates[0]);

        for (int i = 0; i < updates.length; i++) {
            cond = updates[i].split("=");

            Hashtable<String, String> temp = (Hashtable<String, String>) meta[0];
            String type = temp.get(cond[0]);
            if (type.equals("java.lang.Integer")) {

                updateValues.put(cond[0], Integer.parseInt(cond[1]));

            } else if (type.equals("java.lang.Double")) {

                updateValues.put(cond[0], Double.parseDouble(cond[1]));

            } else if (type.equals("java.lang.Date")) {
                updateValues.put(cond[0], new SimpleDateFormat("yyyy-MM-dd").parse(cond[1]));

            } else {// string
                updateValues.put(cond[0], cond[1]);

            }
        }

        updateMethods.updateTable(table, clusterValue, updateValues);
    }

    public static void callInsert(String table, String[] values)
            throws ClassNotFoundException, DBAppException, IOException, ParseException, Exception {

        Object[] meta = updateMethods.getTableInfoMeta(table);
        String[] cond = new String[2];
        Hashtable<String, Object> insertions = new Hashtable<>();

        System.out.println(table);
        System.out.println(values[0]);
        System.out.println(values[1]);
        System.out.println(values[2]);

        for (int i = 0; i < values.length; i++) {
            cond = values[i].split("=");

            Hashtable<String, String> temp = (Hashtable<String, String>) meta[0];
            String type = temp.get(cond[0]);

            if (type.equals("java.lang.Integer")) {

                insertions.put(cond[0], Integer.parseInt(cond[1]));

            } else if (type.equals("java.lang.Double")) {

                insertions.put(cond[0], Double.parseDouble(cond[1]));

            } else if (type.equals("java.lang.Date")) {
                insertions.put(cond[0], new SimpleDateFormat("yyyy-MM-dd").parse(cond[1]));

            } else {// string
                insertions.put(cond[0], cond[1]);

            }
        }

        insertMethods.insertIntoTable(table, insertions);

    }

    public static Iterator convertSql(StringBuffer term) throws Exception {

        String[] sql = term.toString().split(" ");
        // boolean allColumns=false;
        String table = null;

        // Vector<String> values= new Vector<>();
        Vector<String> conditions = new Vector<>();

        Vector<String> operators = new Vector<>();
        String[] values = null;
        // String[] columns= null;

        String indexName = "";
        String[] indexColumns = new String[3];

        if (sql[0].equals("SELECT")) {
            // SELECT * FROM table_name where col1=x AND col2=y

            for (int i = 2; i < sql.length; i++) {
                if (sql[i].equals("FROM")) {
                    table = sql[i + 1];
                }
                if (sql[i].equals("WHERE")) {
                    for (int j = i + 1; j < sql.length; j++) {
                        if (!(sql[j].equals("OR")) && !(sql[j].equals("AND")) && !(sql[j].equals("XOR"))) {
                            conditions.add(sql[j]);
                        } else if (sql[j].equals("OR") || sql[j].equals("AND") || sql[j].equals("XOR")) {
                            operators.add(sql[j]);
                        }
                    }
                }
            }
            return callSelect(table, conditions, operators);
        }

        else if (sql[0].equals("DELETE")) {
            // DELETE FROM table_name WHERE condition
            for (int i = 0; i < sql.length; i++) {
                if (sql[i].equals("FROM")) {
                    table = sql[i + 1];
                } else if (sql[i].equals("WHERE")) {
                    for (int j = i + 1; j < sql.length; j++) {
                        if (!(sql[j].equals("OR")) && !(sql[j].equals("AND")) && !(sql[j].equals("XOR"))) {
                            conditions.add(sql[j]);
                        } else if (sql[j].equals("OR") || sql[j].equals("AND") || sql[j].equals("XOR")) {
                            operators.add(sql[j]);
                        }
                    }
                }
            }
            callDelete(table, conditions, operators);
        }

        else if (sql[0].equals("UPDATE")) {
            String[] cluster = new String[2];
            String[] updates = null;
            // UPDATE table_name
            // SET column1=value1,column2=value2,...
            // WHERE condition;
            table = sql[1];
            for (int i = 0; i < sql.length; i++) {
                if (sql[i].equals("SET")) {
                    updates = sql[i + 1].split(",");
                } else if (sql[i].equals("WHERE")) {
                    cluster = sql[i + 1].split("=");
                    break;
                }
            }

            callUpdate(table, cluster[1], updates);
        }

        else if (sql[0].equals("INSERT")) {
            // INSERT INTO table_name VALUES ( value1,value2,value3,... );
            for (int i = 0; i < sql.length; i++) {
                if (sql[i].equals("INTO")) {
                    table = sql[i + 1];

                } else if (sql[i].equals("VALUES")) {
                    values = sql[i + 2].split(",");
                    break;
                }

            }

            callInsert(table, values);

        }

        else if (sql[0].equals("CREATE") && sql[1].equals("INDEX")) {
            // CREATE INDEX index_name ON table_name ( column1,column2,column3 );
            indexName = sql[2];
            for (int i = 0; i < sql.length; i++) {
                if (sql[i].equals("ON")) {
                    table = sql[i + 1];

                } else if (sql[i].equals("(")) {
                    indexColumns = sql[i + 1].split(",");
                    break;
                }
            }

            callCreateIndex(table, indexColumns);

        }

        else if (sql[0].equals("CREATE") && sql[1].equals("TABLE")) {
            // CREATE TABLE table_name ( column1 datatype, column2 datatype, column3
            // datatype,....);
            table = sql[2];
            Vector<String> types = new Vector<>();
            Vector<String> colNames = new Vector<>();
            String primary = "";

            for (int j = 4; j < sql.length; j++) {
                if (checkType(sql[j])) {
                    types.add(sql[j]);
                } else if (sql[j].equals("PRIMARY")) {
                    primary = sql[j - 1];
                    j++;
                } else {
                    colNames.add(sql[j].substring(0, sql[j].length() - 1));
                }

                if (sql[j].equals(")")) {
                    break;
                }
            }

        }

        return null;
    }

    public static boolean checkType(String type) {
        if (type.equals("int") || type.equals("date")) {
            return true;
        } else if (type.contains("char") || type.contains("double")) {
            return true;
        }
        return false;
    }

    // public static void main(String[] args) throws Exception {
    // Bonus b = new Bonus();
    // StringBuffer s = new StringBuffer("INSERT INTO table_name VALUES ( 3,4,5 )");
    // convertSql(s);
    // }

}