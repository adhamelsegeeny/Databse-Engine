package main.java;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import javax.management.ObjectName;
import javax.print.DocFlavor.STRING;
import javax.xml.crypto.dsig.keyinfo.RetrievalMethod;

public class selectFromMethods {

    public static Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators)
            throws DBAppException, ClassNotFoundException, IOException, ParseException {
        Vector<Hashtable<String, Object>> iterator = new Vector<>();

        validateSelectFromTable(sqlTerms, arrayOperators);
        String tableName = sqlTerms[0].get_strTableName();
        Table table = extractTable("src/main/resources/data/" + tableName + ".ser");
        int index = -1;
        for (int i = 0; i < table.indexs.size(); i++) {
            Index indexTable = table.indexs.get(i);
            if (validateSelectonIndex(sqlTerms, arrayOperators, indexTable))
                index = i;

            if (index != -1)
                break;
        }
        if (index != -1) {
            SQLTerm[] filterSqlTerm = filter(sqlTerms, index, table);

            Vector<RowReference> rowrefrance = useIndex(filterSqlTerm, arrayOperators, table, index);

            Vector<Hashtable<String, Object>> rows = getRowsFromRefarance(rowrefrance, table);
            for (Hashtable<String, Object> row : rows) {

                Vector<Boolean> bool = selectHelper(sqlTerms, row);
                Boolean b = bool.get(0);
                for (int j = 1; j < bool.size(); j++) {
                    switch (arrayOperators[j - 1]) {
                        case "AND":
                            b = b & bool.get(j);
                            break;
                        case "OR":
                            b = b | bool.get(j);
                            break;
                        case "XOR":
                            b = b ^ bool.get(j);
                            break;
                        default:
                            throw new DBAppException("Wrong Operator!");
                    }
                }
                if (b == true) {
                    iterator.add(row);
                }
            }

        } else {
            for (int i = 0; i < table.getPages().size(); i++) {
                String path = table.getPages().get(i).getPath();
                Vector<Hashtable<String, Object>> page = extractPage(path);
                for (Hashtable<String, Object> row : page) {

                    Vector<Boolean> bool = selectHelper(sqlTerms, row);
                    Boolean b = bool.get(0);
                    for (int j = 1; j < bool.size(); j++) {
                        switch (arrayOperators[j - 1]) {
                            case "AND":
                                b = b & bool.get(j);
                                break;
                            case "OR":
                                b = b | bool.get(j);
                                break;
                            case "XOR":
                                b = b ^ bool.get(j);
                                break;
                            default:
                                throw new DBAppException("Wrong Operator!");
                        }
                    }
                    if (b == true) {
                        iterator.add(row);
                    }
                }

            }
        }
        return iterator.iterator();

    }

    private static SQLTerm[] filter(SQLTerm[] sqlTerms, int index, Table table) {

        Index indecice = table.indexs.get(index);
        SQLTerm[] returned = new SQLTerm[] {};
        for (int i = 0; i < sqlTerms.length; i++) {
            SQLTerm term = sqlTerms[i];
            if (term._strColumnName.equals(indecice.index1) || term._strColumnName.equals(indecice.index2)
                    || term._strColumnName.equals(indecice.index3)) {
                returned = Arrays.copyOf(returned, returned.length + 1);
                returned[returned.length - 1] = new SQLTerm(term._strTableName, term._strColumnName, term._strOperator,
                        term._objValue);
            }

        }
        return returned;

    }

    private static Vector<RowReference> useIndex(SQLTerm[] arrSQLTerms, String[] strarrOperators, Table table,
            int number)
            throws IOException, ParseException, DBAppException, ClassNotFoundException {

        Object[] tableInfo = updateMethods.getTableInfoMeta(table.getTableName());
        Hashtable<String, Object> columnMin = (Hashtable<String, Object>) tableInfo[1];
        Hashtable<String, Object> columnMax = (Hashtable<String, Object>) tableInfo[2];

        Index index = table.indexs.get(number);
        Object xMin = columnMin.get(index.index1);
        Object xMax = columnMax.get(index.index1);
        Object yMin = columnMin.get(index.index2);
        Object yMax = columnMax.get(index.index2);
        Object zMin = columnMin.get(index.index3);
        Object zMax = columnMax.get(index.index3);
        for (SQLTerm term : arrSQLTerms) {
            // .out.println(term._strColumnName);
            String col = term._strColumnName;
            String operator = term._strOperator;
            Object value = term._objValue;
            if (operator.equals("=")) {
                if (col.equals(index.index1)) {
                    xMin = value;
                    xMax = value;

                } else if (col.equals(index.index2)) {
                    yMin = value;
                    yMax = value;
                } else {
                    zMin = value;
                    zMax = value;
                }
            } else if (operator.equals(">")) {
                if (col.equals(index.index1)) {
                    xMin = value;
                    xMax = columnMax.get(index.index1);

                } else if (col.equals(index.index2)) {
                    yMin = value;
                    yMax = columnMax.get(index.index2);
                } else {
                    zMin = value;
                    zMax = columnMax.get(index.index3);
                }

            } else if (operator.equals(">=")) {
                if (col.equals(index.index1)) {
                    xMin = value;
                    xMax = columnMax.get(index.index1);

                } else if (col.equals(index.index2)) {
                    yMin = value;
                    yMax = columnMax.get(index.index2);
                } else {
                    zMin = value;
                    zMax = columnMax.get(index.index3);
                }
            } else if (operator.equals("<")) {
                if (col.equals(index.index1)) {
                    xMin = columnMin.get(index.index1);
                    xMax = value;

                } else if (col.equals(index.index2)) {
                    yMin = columnMin.get(index.index2);
                    yMax = value;
                } else {
                    zMin = columnMin.get(index.index3);
                    zMax = value;
                }
            } else if (operator.equals("<=")) {
                if (col.equals(index.index1)) {
                    xMin = columnMin.get(index.index1);
                    xMax = value;

                } else if (col.equals(index.index2)) {
                    yMin = columnMin.get(index.index2);
                    yMax = value;
                } else {
                    zMin = columnMin.get(index.index3);
                    zMax = value;
                }
            }

        }
        Vector<RowReference> result = new Vector<>();
        Index temp = table.indexs.get(number);

        String indexPath = temp.path;
        Node root = updateMethods.getNodefromDisk(indexPath);
        Vector<RowReference> test = root.find(xMin, xMax, yMin, yMax, zMin, zMax);

        result.addAll(test);
        return result;

    }

    private static boolean validateSelectonIndex(SQLTerm[] sqlTerm, String[] strarrOperators, Index index) {
        String index1 = index.index1;
        String index2 = index.index2;
        String index3 = index.index3;
        for (int i = 0; i < sqlTerm.length - 2; i++) {
            SQLTerm term1 = sqlTerm[i];
            SQLTerm term2 = sqlTerm[i + 1];
            SQLTerm term3 = sqlTerm[i + 2];
            if ((index1.equals(term3._strColumnName) || index1.equals(term2._strColumnName)
                    || index1.equals(term1._strColumnName)) &&
                    (index2.equals(term3._strColumnName) || index2.equals(term2._strColumnName)
                            || index2.equals(term1._strColumnName))
                    &&
                    (index3.equals(term3._strColumnName) || index3.equals(term2._strColumnName)
                            || index3.equals(term1._strColumnName)))
                if (strarrOperators[i].equals("AND") && strarrOperators[i + 1].equals("AND")) {
                    Boolean f = true;

                    for (int j = i; j < strarrOperators.length; j++) {
                        if (strarrOperators[j].equals("OR"))
                            f = false;
                    }
                    if (f)
                        return true;
                }

        }
        return false;

    }

    private static Vector<Hashtable<String, Object>> getRowsFromRefarance(Vector<RowReference> result, Table table)
            throws ClassNotFoundException, IOException {

        Vector<Hashtable<String, Object>> rows = new Vector<Hashtable<String, Object>>();
        for (RowReference rowReference : result) {

            for (PageAndRow pageAndRow : rowReference.pageAndRow) {
                String path = "src/main/resources/data/" + table.getName() + pageAndRow.page + ".ser";
                Vector<Hashtable<String, Object>> page = extractPage(path);
                int rowNumber = updateMethods.getRowTarek(page, table.getClusteringKey(),
                        pageAndRow.clustringvalue);
                Hashtable<String, Object> resultRow = page.get(rowNumber);
                rows.add(resultRow);
            }
        }
        return rows;

    }

    public static void validateSelectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        if (arrSQLTerms.length - 1 != strarrOperators.length)
            throw new DBAppException("Number of terms and operators does not match.");

        for (String operator : strarrOperators)
            if (operator != "AND" && operator != "OR" && operator != "XOR")
                throw new DBAppException("No Valid Operator.");

        String TableName = arrSQLTerms[0].get_strTableName();
        for (SQLTerm term : arrSQLTerms)
            if (!term.get_strTableName().equals(TableName))
                throw new DBAppException("The table name in all terms must be the same.");

    }

    private static Table extractTable(String path) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(path);
        ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        Object o = objectIn.readObject();
        objectIn.close();
        fileIn.close();
        return (Table) o;
    }

    private static Vector<Hashtable<String, Object>> extractPage(String path)
            throws IOException, ClassNotFoundException {
        try (FileInputStream fileIn = new FileInputStream(path)) {
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            Object o = objectIn.readObject();
            objectIn.close();
            fileIn.close();
            return (Vector<Hashtable<String, Object>>) o;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public static Vector<Boolean> selectHelper(SQLTerm[] arrSQLTerms, Hashtable<String, Object> row)
            throws DBAppException {

        Vector<Boolean> bool = new Vector<Boolean>();
        for (SQLTerm sqlTerm : arrSQLTerms) {
            switch (sqlTerm._strOperator) {
                case "=":
                    if (row.get(sqlTerm._strColumnName).equals(sqlTerm._objValue))
                        bool.add(true);
                    else
                        bool.add(false);
                    break;
                case "!=":
                    if (!row.get(sqlTerm._strColumnName).equals(sqlTerm._objValue))
                        bool.add(true);
                    else
                        bool.add(false);
                    break;
                case "<=":
                    if (((Comparable) row.get(sqlTerm._strColumnName))
                            .compareTo((Comparable) sqlTerm._objValue) <= 0)
                        bool.add(true);
                    else
                        bool.add(false);
                    break;
                case "<":
                    if (((Comparable) row.get(sqlTerm._strColumnName))
                            .compareTo((Comparable) sqlTerm._objValue) < 0)
                        bool.add(true);
                    else
                        bool.add(false);
                    break;
                case ">=":
                    if (((Comparable) row.get(sqlTerm._strColumnName))
                            .compareTo((Comparable) sqlTerm._objValue) >= 0)
                        bool.add(true);
                    else
                        bool.add(false);
                    break;
                case ">":
                    if (((Comparable) row.get(sqlTerm._strColumnName))
                            .compareTo((Comparable) sqlTerm._objValue) > 0)
                        bool.add(true);
                    else
                        bool.add(false);
                    break;
                default:
                    throw new DBAppException("Wrong Operator!");
            }
        }
        return bool;
    }
}
