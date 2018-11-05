package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.lang.IllegalArgumentException;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {

    //建立表id到表主键的映射
    private HashMap<Integer, String> id2pkey;
    //建立表id到表name的映射
    private HashMap<Integer, String> id2name;
    //建立表id到数据库文件的映射
    private HashMap<Integer, DbFile> id2file;
    //建立表name到表id的映射
    private HashMap<String, Integer> name2id;
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        id2pkey = new HashMap<Integer, String>();
        id2name = new HashMap<Integer, String>();
        id2file = new HashMap<Integer, DbFile>();
        name2id = new HashMap<String, Integer>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        if (name == null || pkeyField == null){
            throw new IllegalArgumentException();
        }
        int tableid = file.getId();
        if(name2id.containsKey(name)){
            throw new UnsupportedOperationException("不支持添加相同名字的table");
        }
        id2file.put(tableid, file);
        id2name.put(tableid, name);
        id2pkey.put(tableid, pkeyField);
        name2id.put(name, tableid);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     *将新表添加到目录中。
     *此表具有使用指定的TupleDesc及其格式化的元组
     *内容存储在指定的DbFile中。
     * @param file 要添加的表的内容; file.getId（）是其标识符
     *此文件/ tupledesc参数用于调用getTupleDesc和getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * 返回具有指定名称的表的id，
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if(name == null || !name2id.containsKey(name)){
            throw new NoSuchElementException();
        }
        return name2id.get(name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * 返回指定表的元组描述符（模式）
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!isIdValid(tableid, id2file)){
            throw new NoSuchElementException();
        }
        return id2file.get(tableid).getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!isIdValid(tableid, id2file)){
            throw new NoSuchElementException();
        }
        return id2file.get(tableid);
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        if (isIdValid(tableid, id2file)){
            throw new NoSuchElementException();
        }
        return id2pkey.get(tableid);
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return id2name.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        if (!isIdValid(id, id2name)){
            throw new NoSuchElementException();
        }
        return id2name.get(id);
    }

    /**键值是否存在*/
    private boolean isIdValid(int id, HashMap<?, ?> map) {
        return map.containsKey(id);
    }


    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        id2name.clear();
        id2pkey.clear();
        id2file.clear();
        name2id.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * 从文件中读取模式，并在数据库中创建相应的表。
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(catalogFile).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

