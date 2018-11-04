package at.htl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class AnimalShelterTest {
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
            String sql = "create table animalshelter (" +
                    "id int constraint animalshelter_pk primary key," +
                    "street varchar(50) not null unique," +
                    "town varchar(50) not null unique," +
                    "post_code int not null unique," +
                    "number_cages int not null" +
                    ")";
            stmt.execute(sql);
            sql = "CREATE TABLE pet (" +
                    "id int constraint pet_pk primary key," +
                    "species varchar(50) not null," +
                    "breed varchar(50) not null," +
                    "name varchar(50)," +
                    "age int not null," +
                    "price double not null" +
                    ")";
            stmt.execute(sql);
            sql = "create table cage(" +
                    "id int constraint cage_pk primary key," +
                    "cage_row int not null," +
                    "cage_column int not null," +
                    "pet_id int not null," +
                    "animalshelter_id int not null," +
                    "constraint fk_cage_pet foreign key (pet_id)" +
                    "references pet(id)," +
                    "constraint fk_cage_animalshelter foreign key (animalshelter_id)" +
                    "references animalshelter(id)" +
                    ")";
            stmt.execute(sql);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        //Daten einfügen
        int countInserts = 0;
        try{
            Statement stmt = conn.createStatement();
            //Animalshelter
            String sql = "insert into animalshelter values (1, 'Am Selnerbach 14', 'Luftenberg', 4225, 100)";
            countInserts += countInserts + stmt.executeUpdate(sql);
            sql = "insert into animalshelter values (2, 'Bachgasse 3', 'Leonding', 4060, 150)";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into animalshelter values (3, 'Sandweg 23', 'Linz', 4141, 50)";
            countInserts += stmt.executeUpdate(sql);

            //Pet
            sql = "insert into pet values (1, 'Hund', 'Dackel', 'Jessy', 5, 100)";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into pet values (2, 'Hund', 'Mischling', 'Rocky', 2, 70)";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into pet values (3, 'Hund', 'SChäferhund', 'Luna', 3, 300)";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into pet values (4, 'Katze', 'Hauskatze', 'Timmy', 1, 20)";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into pet values (5, 'Katze', 'Persische Katze', 'Lilly', 2, 390)";
            countInserts += stmt.executeUpdate(sql);

            //Cage
            sql = "insert into cage values (1, 2, 25, 3, 1)";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into cage values (2, 1, 2, 1, 1)";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into cage values (3, 1, 12, 4, 2)";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into cage values (4, 4, 19, 5, 3)";
            countInserts += stmt.executeUpdate(sql);
            sql = "insert into cage values (5, 3, 8, 2, 2)";
            countInserts += stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        assertThat(countInserts, is(13));
    }

    @AfterClass
    public static void teardownJdbc(){
        try{
            conn.createStatement().execute("DROP TABLE cage");
            conn.createStatement().execute("DROP TABLE animalshelter");
            conn.createStatement().execute("DROP TABLE pet");
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
    public void testAnimalShelter(){
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement("SELECT street, town, post_code, number_cages FROM animalshelter");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("street"), is("Am Selnerbach 14"));
            assertThat(rs.getString("town"), is("Luftenberg"));
            assertThat(rs.getInt("post_code"), is(4225));
            assertThat(rs.getInt("number_cages"), is(100));

            rs.next();
            assertThat(rs.getString("street"), is("Bachgasse 3"));
            assertThat(rs.getString("town"), is("Leonding"));
            assertThat(rs.getInt("post_code"), is(4060));
            assertThat(rs.getInt("number_cages"), is(150));

            rs.next();
            assertThat(rs.getString("street"), is("Sandweg 23"));
            assertThat(rs.getString("town"), is("Linz"));
            assertThat(rs.getInt("post_code"), is(4141));
            assertThat(rs.getInt("number_cages"), is(50));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPet(){
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement("SELECT species, breed, name, age, price FROM pet");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("species"), is("Hund"));
            assertThat(rs.getString("breed"), is("Dackel"));
            assertThat(rs.getString("name"), is("Jessy"));
            assertThat(rs.getInt("age"), is(5));
            assertThat(rs.getDouble("price"), is(100.0));

            rs.next();
            assertThat(rs.getString("species"), is("Hund"));
            assertThat(rs.getString("breed"), is("Mischling"));
            assertThat(rs.getString("name"), is("Rocky"));
            assertThat(rs.getInt("age"), is(2));
            assertThat(rs.getDouble("price"), is(70.0));

            rs.next();
            assertThat(rs.getString("species"), is("Hund"));
            assertThat(rs.getString("breed"), is("SChäferhund"));
            assertThat(rs.getString("name"), is("Luna"));
            assertThat(rs.getInt("age"), is(3));
            assertThat(rs.getDouble("price"), is(300.0));

            rs.next();
            assertThat(rs.getString("species"), is("Katze"));
            assertThat(rs.getString("breed"), is("Hauskatze"));
            assertThat(rs.getString("name"), is("Timmy"));
            assertThat(rs.getInt("age"), is(1));
            assertThat(rs.getDouble("price"), is(20.0));

            rs.next();
            assertThat(rs.getString("species"), is("Katze"));
            assertThat(rs.getString("breed"), is("Persische Katze"));
            assertThat(rs.getString("name"), is("Lilly"));
            assertThat(rs.getInt("age"), is(2));
            assertThat(rs.getDouble("price"), is(390.0));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCage(){
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement("SELECT cage_row, cage_column, pet_id, animalshelter_id FROM cage");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getInt("cage_row"), is(2));
            assertThat(rs.getInt("cage_column"), is(25));
            assertThat(rs.getInt("pet_id"), is(3));
            assertThat(rs.getInt("animalshelter_id"), is(1));

            rs.next();
            assertThat(rs.getInt("cage_row"), is(1));
            assertThat(rs.getInt("cage_column"), is(2));
            assertThat(rs.getInt("pet_id"), is(1));
            assertThat(rs.getInt("animalshelter_id"), is(1));

            rs.next();
            assertThat(rs.getInt("cage_row"), is(1));
            assertThat(rs.getInt("cage_column"), is(12));
            assertThat(rs.getInt("pet_id"), is(4));
            assertThat(rs.getInt("animalshelter_id"), is(2));

            rs.next();
            assertThat(rs.getInt("cage_row"), is(4));
            assertThat(rs.getInt("cage_column"), is(19));
            assertThat(rs.getInt("pet_id"), is(5));
            assertThat(rs.getInt("animalshelter_id"), is(3));

            rs.next();
            assertThat(rs.getInt("cage_row"), is(3));
            assertThat(rs.getInt("cage_column"), is(8));
            assertThat(rs.getInt("pet_id"), is(2));
            assertThat(rs.getInt("animalshelter_id"), is(2));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMetaDataAnimalshelter(){
        try{
            DatabaseMetaData dbMetaData = conn.getMetaData();
            String tableName = "ANIMALSHELTER";
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

            columns.next();
            String columnName = columns.getString("COLUMN_NAME");
            String datatype = columns.getString("DATA_TYPE");
            String columnsize = columns.getString("COLUMN_SIZE");
            String isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("ID"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("STREET"));
            assertThat(Integer.parseInt(datatype), is(Types.VARCHAR));
            assertThat(Integer.parseInt(columnsize), is(50));
            assertThat(isNullable, is("NO"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("TOWN"));
            assertThat(Integer.parseInt(datatype), is(Types.VARCHAR));
            assertThat(Integer.parseInt(columnsize), is(50));
            assertThat(isNullable, is("NO"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("POST_CODE"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("NUMBER_CAGES"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            //Primary Key
            ResultSet pk = dbMetaData.getPrimaryKeys(null,null, tableName);
            pk.next();
            columnName = pk.getString("COLUMN_NAME");
            String pkName = pk.getString("PK_NAME");
            assertThat(columnName, is("ID"));
            assertThat(pkName, is("ANIMALSHELTER_PK"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMetaDataPet(){
        try{
            DatabaseMetaData dbMetaData = conn.getMetaData();
            String tableName = "PET";
            ResultSet columns = dbMetaData.getColumns(null, null, tableName, null);

            columns.next();
            String columnName = columns.getString("COLUMN_NAME");
            String datatype = columns.getString("DATA_TYPE");
            String columnsize = columns.getString("COLUMN_SIZE");
            String isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("ID"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("SPECIES"));
            assertThat(Integer.parseInt(datatype), is(Types.VARCHAR));
            assertThat(Integer.parseInt(columnsize), is(50));
            assertThat(isNullable, is("NO"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("BREED"));
            assertThat(Integer.parseInt(datatype), is(Types.VARCHAR));
            assertThat(Integer.parseInt(columnsize), is(50));
            assertThat(isNullable, is("NO"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("NAME"));
            assertThat(Integer.parseInt(datatype), is(Types.VARCHAR));
            assertThat(Integer.parseInt(columnsize), is(50));
            assertThat(isNullable, is("YES"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("AGE"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("PRICE"));
            assertThat(Integer.parseInt(datatype), is(Types.DOUBLE));
            assertThat(Integer.parseInt(columnsize), is(52));
            assertThat(isNullable, is("NO"));

            //Primary Key
            ResultSet pk = dbMetaData.getPrimaryKeys(null,null, tableName);
            pk.next();
            columnName = pk.getString("COLUMN_NAME");
            String pkName = pk.getString("PK_NAME");
            assertThat(columnName, is("ID"));
            assertThat(pkName, is("PET_PK"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMetaDataCage(){
        try{
            DatabaseMetaData dbMetaData = conn.getMetaData();
            String tableName = "CAGE";
            ResultSet columns = dbMetaData.getColumns(null, null, tableName, null);

            columns.next();
            String columnName = columns.getString("COLUMN_NAME");
            String datatype = columns.getString("DATA_TYPE");
            String columnsize = columns.getString("COLUMN_SIZE");
            String isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("ID"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("CAGE_ROW"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("CAGE_COLUMN"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("PET_ID"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            columns.next();
            columnName = columns.getString("COLUMN_NAME");
            datatype = columns.getString("DATA_TYPE");
            columnsize = columns.getString("COLUMN_SIZE");
            isNullable = columns.getString("IS_NULLABLE");
            assertThat(columnName, is("ANIMALSHELTER_ID"));
            assertThat(Integer.parseInt(datatype), is(Types.INTEGER));
            assertThat(Integer.parseInt(columnsize), is(10));
            assertThat(isNullable, is("NO"));

            //Primary Key
            ResultSet pk = dbMetaData.getPrimaryKeys(null,null, tableName);
            pk.next();
            columnName = pk.getString("COLUMN_NAME");
            String pkName = pk.getString("PK_NAME");
            assertThat(columnName, is("ID"));
            assertThat(pkName, is("CAGE_PK"));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
