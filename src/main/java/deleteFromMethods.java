package main.java;

import java.beans.IndexedPropertyDescriptor;
import java.beans.Transient;
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

public class deleteFromMethods {

	public static void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue)
			throws Exception {

		Object[] tableInfo = updateMethods.getTableInfoMeta(tableName); // info about table
		Hashtable<String, String> colDataTypes = (Hashtable<String, String>) tableInfo[0];
		Hashtable<String, Object> colMin = (Hashtable<String, Object>) tableInfo[1];
		Hashtable<String, Object> colMax = (Hashtable<String, Object>) tableInfo[2];

		String clusteringCol = (String) tableInfo[4]; // clustering column name

		String path = "src/main/resources/data/" + tableName + ".ser"; // table path
		Table table = updateMethods.getTablefromCSV(path);

		// checks the compatibility
		for (String key : columnNameValue.keySet()) {
			if (!colDataTypes.containsKey(key)) {
				throw new DBAppException("column not found!");
			}
			Class columnClass = Class.forName(colDataTypes.get(key));
			if (!columnClass.isInstance(columnNameValue.get(key))) {
				throw new DBAppException("Data types do not match!");
			}
		}

		// check the values ranges
		for (String key : columnNameValue.keySet()) {
			switch (colDataTypes.get(key)) {
				case "java.lang.Integer":
					if (((Integer) columnNameValue.get(key)).compareTo((Integer) colMin.get(key)) < 0) {
						throw new DBAppException("the value of the column " + key + " is below range!");
					}
					if (((Integer) columnNameValue.get(key)).compareTo((Integer) colMax.get(key)) > 0) {
						throw new DBAppException("the value of the column " + key + " is above range!");

					}
					break;
				case "java.lang.Double":
					if (((Double) columnNameValue.get(key)).compareTo((Double) colMin.get(key)) < 0) {
						throw new DBAppException("the value of the column " + key + " is below range!");
					}
					if (((Double) columnNameValue.get(key)).compareTo((Double) colMax.get(key)) > 0) {
						throw new DBAppException("the value of the column " + key + " is above range!");
					}
					break;
				case "java.util.Date":
					if (((Date) columnNameValue.get(key)).compareTo((Date) colMin.get(key)) < 0) {
						throw new DBAppException("the value of the column " + key + " is below range!");
					}
					if (((Date) columnNameValue.get(key)).compareTo((Date) colMax.get(key)) > 0) {
						throw new DBAppException("the value of the column " + key + " is above range!");
					}
					break;
				default:
					if (((String) columnNameValue.get(key)).compareTo((String) colMin.get(key)) < 0) {
						throw new DBAppException("Value for column " + key + " is below the minimum allowed. Min: "
								+ colMin.get(key) + ". Found: " + columnNameValue.get(key));
					}
					if (((String) columnNameValue.get(key)).compareTo((String) colMax.get(key)) > 0) {
						throw new DBAppException("Value for column " + key + " is above the maximum allowed. Max: "
								+ colMax.get(key) + ". Found: " + columnNameValue.get(key));
					}
					break;
			}

		}

		helper(columnNameValue, clusteringCol, path, table, colMin);
		IndexMethods.updateIndex(tableName);

	}

	/**
	 * @param columnNameValue
	 * @param clusteringCol
	 * @param path
	 * @param table
	 * @param colMin
	 * @throws DBAppException
	 * @throws Exception
	 */
	private static void helper(Hashtable<String, Object> columnNameValue, String clusteringCol, String path,
			Table table, Hashtable<String, Object> colMin) throws DBAppException, Exception {
		boolean isCluster = columnNameValue.get(clusteringCol) != null; // true

		boolean flag = false;
		int numberIndex = insertMethods.returnIndex("src/main/resources/data/" + table.getTableName() + ".ser",
				columnNameValue);

		if (numberIndex != -1) {
			Index index = table.indexs.get(numberIndex);
			Node root = updateMethods.getNodefromDisk(index.path);
			Object x = columnNameValue.get(index.index1);
			Object y = columnNameValue.get(index.index2);
			Object z = columnNameValue.get(index.index3);
			RowReference refrances = root.find(x, y, z);

			// .out.println("her");
			// .out.println(root.points.size());
			// .out.println(x + " " + y + " " + z);
			// .out.println(
			// refrances.x + " " + refrances.y + " " + refrances.z + " " +
			// refrances.pageAndRow.get(0).page);
			// Vector<Hashtable<String, Object>> rows = new Vector<>();
			// .out.println(refrances.pageAndRow.size());
			Vector<PageAndRow> refrancess = refrances.pageAndRow;

			for (int j = 0; j < refrances.pageAndRow.size(); j++) {
				PageAndRow single = refrances.pageAndRow.get(j);
				int pageNumber = single.page;
				Object cluster = single.clustringvalue;

				String pagePath = table.getPages().get(pageNumber).getPath(); // page path
				Vector<Hashtable<String, Object>> page = updateMethods.getPagesfromCSV(pagePath);
				int rowNumber = updateMethods.getRowTarek(page, clusteringCol, cluster);
				if (!checkCommne(page.get(rowNumber),
						columnNameValue)) {
					throw new DBAppException("Data not found");
				}

				if (rowNumber == -1) {
					throw new DBAppException("No row Found");
				}

				flag = true;
				Hashtable<String, Object> forIndex = page.get(rowNumber);
				page.remove(rowNumber);
				table.getPages().get(pageNumber)
						.setNumberofRecords(table.getPages().get(pageNumber).getNumberofRecords() - 1);

				insertMethods.writeIntoDisk(page, pagePath);
				insertMethods.writeIntoDisk(table, "src/main/resources/data/" + table.getTableName() + ".ser");
				if (forIndex != null && IndexMethods.columnIndexs(forIndex, path) != null) {
					int number = insertMethods.returnIndex("src/main/resources/data/" + table.getName() + ".ser",
							forIndex);
					Index index1 = table.indexs.get(number);

					String nodePath = index1.path;
					Node root1 = updateMethods.getNodefromDisk(nodePath);
					Vector<Object> v = IndexMethods.columnIndexs(forIndex, path);

					root.deleteRowrefrance(v.get(0), v.get(1), v.get(2), pageNumber,
							forIndex.get(table.getClusteringKey()));
					deleteFromMethods.serialize(root1, nodePath);

				}
				if (page.size() == 0) {
					// you have to delete page
					File f = new File(pagePath);
					f.delete();
					table.getPages().remove(pageNumber);

					insertMethods.writeIntoDisk(table, "src/main/resources/data/" + table.getTableName() + ".ser");

					for (int i = 0; i < table.getPages().size(); i++) {
						Page p = table.getPages().get(i);
						Vector<Hashtable<String, Object>> v = updateMethods.getPagesfromCSV(p.getPath());
						File newFile = new File(p.getPath());
						newFile.delete();
						p.setPath("src/main/resources/data/" + table.getTableName() + i + ".ser");
						serialize(v, p.getPath());
						insertMethods.writeIntoDisk(table, "src/main/resources/data/" + table.getTableName() + ".ser");
					}

				}
			}
		}

		else if (isCluster) {
			// binary search for the clustering key
			int pageNumber = updateMethods.getPageTarek(table.getPages(), columnNameValue.get(clusteringCol));
			if (pageNumber == -1) {
				throw new DBAppException("errorHer");
			}

			else if (table.getPages().size() <= pageNumber) {
				return;
			}

			String pagePath = table.getPages().get(pageNumber).getPath(); // page path
			Vector<Hashtable<String, Object>> page = updateMethods.getPagesfromCSV(pagePath);
			int rowNumber = updateMethods.getRowTarek(page, clusteringCol, columnNameValue.get(clusteringCol)); // to
																												// be
			if (!checkCommne(page.get(rowNumber),
					columnNameValue)) {
				throw new DBAppException("Data not found");
			}

			if (rowNumber == -1) {
				throw new DBAppException("No row Found");
			}
			// check for matching data
			flag = true;
			Hashtable<String, Object> forIndex = page.get(rowNumber);
			page.remove(rowNumber);
			table.getPages().get(pageNumber)
					.setNumberofRecords(table.getPages().get(pageNumber).getNumberofRecords() - 1);

			insertMethods.writeIntoDisk(page, pagePath);
			insertMethods.writeIntoDisk(table, "src/main/resources/data/" + table.getTableName() + ".ser");
			if (forIndex != null && IndexMethods.columnIndexs(forIndex, path) != null) {
				int number = insertMethods.returnIndex("src/main/resources/data/" + table.getName() + ".ser",
						forIndex);
				Index index = table.indexs.get(number);

				String nodePath = index.path;
				Node root = updateMethods.getNodefromDisk(nodePath);
				Vector<Object> v = IndexMethods.columnIndexs(forIndex, path);
				root.deleteRowrefrance(v.get(0), v.get(1), v.get(2), pageNumber,
						forIndex.get(table.getClusteringKey()));
				deleteFromMethods.serialize(root, nodePath);

			}
			if (page.size() == 0) {
				// you have to delete page
				File f = new File(pagePath);
				f.delete();
				table.getPages().remove(pageNumber);

				insertMethods.writeIntoDisk(table, "src/main/resources/data/" + table.getTableName() + ".ser");

				for (int i = 0; i < table.getPages().size(); i++) {
					Page p = table.getPages().get(i);
					Vector<Hashtable<String, Object>> v = updateMethods.getPagesfromCSV(p.getPath());
					File newFile = new File(p.getPath());
					newFile.delete();
					p.setPath("src/main/resources/data/" + table.getTableName() + i + ".ser");
					serialize(v, p.getPath());
					insertMethods.writeIntoDisk(table, "src/main/resources/data/" + table.getTableName() + ".ser");
				}

			} else {
				insertMethods.updateRange(table, pagePath);

			}
		} else if (!isCluster) {
			// linear search for matching records

			for (int i = 0; i < table.getPages().size(); i++) {// every page
				String pagePath = table.getPages().get(i).getPath(); // page path

				Vector<Hashtable<String, Object>> page = updateMethods.getPagesfromCSV(pagePath);

				for (int j = 0; j < page.size(); j++) {// every row
					Hashtable<String, Object> row = page.get(j);
					for (String key : columnNameValue.keySet()) {// every Column

						if (compare(row.get(key), columnNameValue.get(key)) == 0
								&& checkCommne(row, columnNameValue)) {
							flag = true;
							page.remove(j);

							table.getPages().get(i)
									.setNumberofRecords(table.getPages().get(i).getNumberofRecords() - 1);
							j--;

							break;

						}

					}

				}

				if (page.size() == 0) {
					// you have to delete page
					File f = new File(pagePath);
					f.delete();
					table.getPages().remove(i);

					insertMethods.writeIntoDisk(table, "src/main/resources/data/" + table.getTableName() + ".ser");
					IndexMethods.updateIndex(table.getTableName());
					for (int k = 0; k < table.getPages().size(); k++) {
						Page p = table.getPages().get(k);
						Vector<Hashtable<String, Object>> v = updateMethods.getPagesfromCSV(p.getPath());
						File newFile = new File(p.getPath());
						newFile.delete();
						p.setPath("src/main/resources/data" + table.getTableName() + k + ".ser");
						serialize(v, p.getPath());
						insertMethods.writeIntoDisk(table,
								"src/main/resources/data/" + table.getTableName() + ".ser");
					}
					i--;
					// end of the page check for the page size

				} else {
					insertMethods.writeIntoDisk(page, pagePath);

					insertMethods.writeIntoDisk(table, "src/main/resources/data/" + table.getTableName() + ".ser");

					insertMethods.updateRange(table, pagePath);

				}
			}
		}

		if (!flag)

		{
			throw new DBAppException("Record not found!");
		}

	}

	private static boolean checkCommne(Hashtable<String, Object> hashtable, Hashtable<String, Object> columnNameValue) {
		for (String key : hashtable.keySet()) {

			if (columnNameValue.containsKey(key)) {
				if (compare(hashtable.get(key), columnNameValue.get(key)) != 0)
					return false;

			}

		}
		return true;
	}

	static void serialize(Object o, String path) throws IOException {

		FileOutputStream tableRewrite = new FileOutputStream(path);
		ObjectOutputStream out = new ObjectOutputStream(tableRewrite);
		out.writeObject(o);
		out.close();
		tableRewrite.close();
	}

	private static int compare(Object clusteringObject1, Object clusteringObject2) {
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

}
