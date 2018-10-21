package at.htl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class BooksAdministrationTest {
    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db;create=true";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    private static Connection conn;

    @BeforeClass
    public static void initJdbc(){
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank nicht möglich\n" + e.getMessage() + "\n");
            System.exit(1);
        }

        //Tabellen erstellen
        try {
            Statement stmt = conn.createStatement();
            String sql = "create table book (" +
                    "isbn int constraint book_pk primary key," +
                    "title varchar(50) not null," +
                    "author varchar(50) not null," +
                    "genre varchar(50) not null unique " +
                    ")";
            stmt.execute(sql);
            sql = "create table rental(" +
                    "rental_id int constraint rental_pk primary key," +
                    "isbn int not null," +
                    "rental_date date not null," +
                    "return_date date not null," +
                    "constraint fk_rental_book foreign key (isbn)" +
                    "references book(isbn)" +
                    ")";
            stmt.execute(sql);
            sql = "CREATE TABLE owner (" +
                    "owner_name varchar(50)," +
                    "rental_id int not null," +
                    "constraint pk_owner primary key (owner_name, rental_id)," +
                    "constraint fk_owner_rental foreign key (rental_id)" +
                    "references rental(rental_id)" +
                    ")";
            stmt.execute(sql);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        //Daten einfügen
        int countInserts = 0;
        try{
            Statement stmt = conn.createStatement();
            //Book
            String sql = "insert into book (isbn, title, author, genre) values (978360893, 'Herr der Ringe', 'J. R. R. Tolkien', 'Fantasy')";
            countInserts += countInserts + stmt.executeUpdate(sql);
            sql = "insert into book (isbn, title, author, genre) values (836089382, 'Star Wars', 'George Lucas', 'Science-Fiction')";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into book (isbn, title, author, genre) values (508938289, 'Die Angst schläft nie', 'Rachel Claine', 'Krimi')";
            countInserts += stmt.executeUpdate(sql);

            //Rental
            sql = "insert into rental (rental_id, isbn, rental_date, return_date) values (1, 978360893, '23.5.2018', '23.7.2018')";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into rental (rental_id, isbn, rental_date, return_date) values (2, 836089382, '1.7.2018', '23.7.2018')";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into rental (rental_id, isbn, rental_date, return_date) values (3, 508938289, '2.4.2018', '21.11.2018')";
            countInserts += stmt.executeUpdate(sql);

            //Owner
            sql = "insert into owner (owner_name, rental_id) values ('Lukas', 1)";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into owner (owner_name, rental_id) values ('Leon', 2)";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into owner (owner_name, rental_id) values ('Michael', 3)";
            countInserts += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        assertThat(countInserts, is(9));
    }

    @AfterClass
    public static void teardownJdbc(){
        try{
            conn.createStatement().execute("DROP TABLE owner");
            conn.createStatement().execute("DROP TABLE rental");
            conn.createStatement().execute("DROP TABLE book");
            System.out.println("Tabellen gelöscht");
        } catch (SQLException e) {
            System.out.println("Tabllen konnten nicht gelöscht werden:\n" + e.getMessage());
        }
        try {
            if(conn != null && !conn.isClosed()){
                conn.close();
                System.out.println("Good bye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBook(){
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement("SELECT isbn, title, author, genre FROM book");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getInt("isbn"), is(978360893));
            assertThat(rs.getString("title"), is("Herr der Ringe"));
            assertThat(rs.getString("author"), is("J. R. R. Tolkien"));
            assertThat(rs.getString("genre"), is("Fantasy"));

            rs.next();
            assertThat(rs.getInt("isbn"), is(836089382));
            assertThat(rs.getString("title"), is("Star Wars"));
            assertThat(rs.getString("author"), is("George Lucas"));
            assertThat(rs.getString("genre"), is("Science-Fiction"));

            rs.next();
            assertThat(rs.getInt("isbn"), is(508938289));
            assertThat(rs.getString("title"), is("Die Angst schläft nie"));
            assertThat(rs.getString("author"), is("Rachel Claine"));
            assertThat(rs.getString("genre"), is("Krimi"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRental(){
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement("SELECT isbn, rental_date, return_date FROM rental");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getInt("isbn"), is(978360893));
            assertThat(rs.getString("rental_date"), is("2018-05-23"));
            assertThat(rs.getString("return_date"), is("2018-07-23"));

            rs.next();
            assertThat(rs.getInt("isbn"), is(836089382));
            assertThat(rs.getString("rental_date"), is("2018-07-01"));
            assertThat(rs.getString("return_date"), is("2018-07-23"));

            rs.next();
            assertThat(rs.getInt("isbn"), is(508938289));
            assertThat(rs.getString("rental_date"), is("2018-04-02"));
            assertThat(rs.getString("return_date"), is("2018-11-21"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testOwner(){
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement("SELECT owner_name, rental_id FROM owner");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("owner_name"), is("Leon"));
            assertThat(rs.getInt("rental_id"), is(2));

            rs.next();
            assertThat(rs.getString("owner_name"), is("Lukas"));
            assertThat(rs.getInt("rental_id"), is(1));

            rs.next();
            assertThat(rs.getString("owner_name"), is("Michael"));
            assertThat(rs.getInt("rental_id"), is(3));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMetaDataBook(){
        try{
            DatabaseMetaData dbMetaData = conn.getMetaData();
            String tableName = "BOOK";
            ResultSet columns = dbMetaData.getColumns(null, null, tableName, null);

            /*while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String datatype = columns.getString("DATA_TYPE");
                String columnsize = columns.getString("COLUMN_SIZE");
                String decimaldigits = columns.getString("DECIMAL_DIGITS");
                String isNullable = columns.getString("IS_NULLABLE");
                String is_autoIncrment = columns.getString("IS_AUTOINCREMENT");
                System.out.println(columnName + "---" + datatype + "---" + columnsize + "---" + decimaldigits + "---" + isNullable + "---" + is_autoIncrment);
                System.out.println("\n" + columns.getString(4) + " " + columns.getInt(5));
            }*/

            //ISBN
            columns.next();
            String columnName = columns.getString("COLUMN_NAME");
            String datatype = columns.getString("DATA_TYPE");
            String columnsize = columns.getString("COLUMN_SIZE");
            String isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("ISBN"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            //Title
            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("TITLE"));
            assertThat(Integer.parseInt(datatype), is(Types.VARCHAR));
            assertThat(Integer.parseInt(columnsize), is(50));
            assertThat(isNullable, is("NO"));

            //Author
            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("AUTHOR"));
            assertThat(Integer.parseInt(datatype), is(Types.VARCHAR));
            assertThat(Integer.parseInt(columnsize), is(50));
            assertThat(isNullable, is("NO"));

            //Genre
            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("GENRE"));
            assertThat(Integer.parseInt(datatype), is(Types.VARCHAR));
            assertThat(Integer.parseInt(columnsize), is(50));
            assertThat(isNullable, is("NO"));

            //Primary Key
            ResultSet pk = dbMetaData.getPrimaryKeys(null,null, tableName);
            pk.next();
            columnName = pk.getString("COLUMN_NAME");
            String pkName = pk.getString("PK_NAME");
            assertThat(columnName, is("ISBN"));
            assertThat(pkName, is("BOOK_PK"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMetaDataRental(){
        try{
            DatabaseMetaData dbMetaData = conn.getMetaData();
            String tableName = "RENTAL";
            ResultSet columns = dbMetaData.getColumns(null, null, tableName, null);

            //Rental_id
            columns.next();
            String columnName = columns.getString("COLUMN_NAME");
            String datatype = columns.getString("DATA_TYPE");
            String columnsize = columns.getString("COLUMN_SIZE");
            String isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("RENTAL_ID"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            //ISBN
            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("ISBN"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            //Rental_date
            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("RENTAL_DATE"));
            assertThat(Integer.parseInt(datatype), is(Types.DATE));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            //Return_date
            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("RETURN_DATE"));
            assertThat(Integer.parseInt(datatype), is(Types.DATE));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            //Primary Key
            ResultSet pk = dbMetaData.getPrimaryKeys(null,null, tableName);
            pk.next();
            columnName = pk.getString("COLUMN_NAME");
            String pkName = pk.getString("PK_NAME");
            assertThat(columnName, is("RENTAL_ID"));
            assertThat(pkName, is("RENTAL_PK"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMetaDataOwner(){
        try{
            DatabaseMetaData dbMetaData = conn.getMetaData();
            String tableName = "OWNER";
            ResultSet columns = dbMetaData.getColumns(null, null, tableName, null);

            //Owner_name
            columns.next();
            String columnName = columns.getString("COLUMN_NAME");
            String datatype = columns.getString("DATA_TYPE");
            String columnsize = columns.getString("COLUMN_SIZE");
            String isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("OWNER_NAME"));
            assertThat(Integer.parseInt(datatype), is(Types.VARCHAR));
            assertThat(Integer.parseInt(columnsize), is(50));
            assertThat(isNullable, is("NO"));

            //Rental_id
            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("RENTAL_ID"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            //Primary Key
            ResultSet pk = dbMetaData.getPrimaryKeys(null,null, tableName);
            pk.next();
            columnName = pk.getString("COLUMN_NAME");
            String pkName = pk.getString("PK_NAME");
            assertThat(columnName, is("OWNER_NAME"));
            assertThat(pkName, is("PK_OWNER"));

            pk.next();
            columnName = pk.getString("COLUMN_NAME");
            pkName = pk.getString("PK_NAME");
            assertThat(columnName, is("RENTAL_ID"));
            assertThat(pkName, is("PK_OWNER"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
