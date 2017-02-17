import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;

/** Databases.java
 * Databases is a class for Fundamentals: Databases mid-semester assessment
 * "Creating and Accessing Databases"
 *
 * The task was to read in a given .CSV file, to create a database in a sensible format
 * to store the data in, and then access it through a Java program to answer some questions.
 *
 * Note: The whole program takes approximately 5 minutes including creating the tables and populating
 * them with data for the first time. Methods can be commented out from main() to negate the need to
 * rebuild the database eachh time.
 *
 * Created by Robert Campbell on 16/02/2017.
 */
public class Databases {
    private Connection db = null;
    private String url;
    private String username;
    private String password;
    private ArrayList<String[]> results = new ArrayList<>();

    /** Constructor for Databases class
     *
     * @param url the url of the database you wish to access
     * @param username your username for accessing the database
     * @param password your password for accessing the database
     */
    public Databases(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    /** startConnection is a method which attempts to make a connection
     * with the database using the postgresql Driver.
     *
     * If a connection cannot be made an SQLException is thrown
     */
    public void startConnection() {
        try {
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException ex) {
            System.out.println("Driver not found");
        }
        try {
            db = DriverManager.getConnection("jdbc:postgresql://" + url, username, password);
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }

        if (db != null) {
            System.out.println("Database accessed!");
        }
        else {
            System.out.println("Failed to make connection");
        }
    }

    /** closeConnection is a method which closes the connection to the database
     *
     * @throws SQLException
     */
    public void closeConnection() throws SQLException {
        if (db != null) {
            db.close();
        }
    }

    /** readCSV is a method which takes a comma separated values file and
     * reads the data contained within line by line using a BufferedReader.
     * It stores this data in an ArrayList<String> to be used by other methods
     * when populating the relevant tables in the database.
     *
     * @param filename the filepath of the .csv file
     */
    public void readCSV(String filename) {
        String line;
        String[] token;

        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            br.readLine();
            while((line = br.readLine()) != null) {
//                Ignore comma's contained within data cells
                token = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
//                Remove *'s from the end of any pieces of data
//                (Quite costly but makes the tags easier to deal with)
                for (int i = 3; i < token.length; i++) {
                    if (token[i].endsWith("*")) { token[i] = token[i].substring(0, token[i].length()-1); }
                }
                results.add(token);
            }
            br.close();
        }
        catch (FileNotFoundException e){
            System.out.println("File not found");
        }

        catch (IOException ex) {
            System.out.println(ex.getMessage()+"Error reading file");
        }
    }

    /** createTables is a method which creates the tables needed for the
     * database outlined in the included ER-diagram
     *
     * @throws SQLException
     */
    public void createTables() throws SQLException {
        PreparedStatement createArtist = db.prepareStatement("CREATE TABLE artist(" +
                "id SERIAL PRIMARY KEY, " +
                "name TEXT NOT NULL UNIQUE);");
        createArtist.execute();
        createArtist.close();

        PreparedStatement createTempAlbum = db.prepareStatement("CREATE TABLE tempAlbum(" +
                "id SERIAL PRIMARY KEY, " +
                "name TEXT NOT NULL, " +
                "artistID INT);");
        createTempAlbum.execute();
        createTempAlbum.close();

        PreparedStatement createAlbum = db.prepareStatement("CREATE TABLE album(" +
                "id SERIAL PRIMARY KEY, " +
                "name TEXT NOT NULL, " +
                "artistID INT REFERENCES artist(id));");
        createAlbum.execute();
        createAlbum.close();

        PreparedStatement createTag = db.prepareStatement("CREATE TABLE tag(" +
                "id SERIAL PRIMARY KEY, " +
                "name TEXT NOT NULL);");
        createTag.execute();
        createTag.close();

        PreparedStatement createSong = db.prepareStatement("CREATE TABLE song(" +
                "id SERIAL PRIMARY KEY, " +
                "name TEXT NOT NULL, " +
                "albumID INT REFERENCES album(id), " +
                "artistID INT REFERENCES artist(id));");
        createSong.execute();
        createSong.close();

        PreparedStatement createSongTag = db.prepareStatement("CREATE TABLE songTag(" +
                "id SERIAL PRIMARY KEY, " +
                "songID INT REFERENCES song(id), " +
                "tagID INT REFERENCES tag(id));");
        createSongTag.execute();
        createSongTag.close();
    }

    /** cleanTables is a method which resets all the tables back to their initial states
     * i.e. ALL DATA IS REMOVED AND SEQUENCES ARE RESTARTED!
     *
     * @throws SQLException
     */
    public void cleanTables() throws SQLException {
        PreparedStatement cleanSongtagTable = db.prepareStatement("DELETE FROM songtag WHERE id > 0; ALTER SEQUENCE songtag_id_seq RESTART");
        cleanSongtagTable.execute();
        cleanSongtagTable.close();

        PreparedStatement cleanSongTable = db.prepareStatement("DELETE FROM song WHERE id > 0; ALTER SEQUENCE song_id_seq RESTART");
        cleanSongTable.execute();
        cleanSongTable.close();

        PreparedStatement cleanAlbumTable = db.prepareStatement("DELETE FROM album WHERE id > 0; ALTER SEQUENCE album_id_seq RESTART");
        cleanAlbumTable.execute();
        cleanAlbumTable.close();

        PreparedStatement cleanTempAlbumTable = db.prepareStatement("DELETE FROM tempalbum WHERE id > 0; ALTER SEQUENCE tempalbum_id_seq RESTART");
        cleanTempAlbumTable.execute();
        cleanTempAlbumTable.close();

        PreparedStatement cleanTagTable = db.prepareStatement("DELETE FROM tag WHERE id > 0; ALTER SEQUENCE tag_id_seq RESTART");
        cleanTagTable.execute();
        cleanTagTable.close();

        PreparedStatement cleanArtistTable = db.prepareStatement("DELETE FROM artist WHERE id > 0; ALTER SEQUENCE artist_id_seq RESTART");
        cleanArtistTable.execute();
        cleanArtistTable.close();
    }

    /** firstPass is a method that iterates through the arraylist of results created after
     * reading in the CSV file and then uses the data contained within to populate the
     * artist, album, and tag tables of the database.
     *
     * @throws SQLException
     */
    public void firstPass() throws SQLException {
        String artistName, albumName;

        PreparedStatement addArtist = db.prepareStatement("INSERT INTO artist (name) SELECT (?) WHERE NOT EXISTS (SELECT * FROM artist WHERE name = (?))");
        PreparedStatement addTempAlbum = db.prepareStatement("INSERT INTO tempAlbum (name, artistid) VALUES (?, (SELECT id FROM artist WHERE name = ?))");
        PreparedStatement addTag = db.prepareStatement("INSERT INTO tag (name) SELECT (?) WHERE NOT EXISTS (SELECT * FROM tag WHERE name = (?))");
        PreparedStatement addAlbum = db.prepareStatement("INSERT INTO album (name, artistid) SELECT DISTINCT name, artistid FROM tempalbum");

        for (String[] s : results) {
            // Add all the artists to a table
            artistName = s[1];
            albumName = s[2];

            addArtist.setString(1, artistName);
            addArtist.setString(2, artistName);
            addArtist.execute();

//            Add all the albums to a temporary table
            addTempAlbum.setString(1, albumName);
            addTempAlbum.setString(2, artistName);
            addTempAlbum.execute();

            // Add all the tags to a table
            for (int i = 3; i < s.length; i++) {
                if (s.length >= 4 && s[i].length() != 0) {
                    addTag.setString(1, s[i]);
                    addTag.setString(2, s[i]);
                    addTag.execute();
                }
            }
        }
        addAlbum.execute();
//        Drop the temporary album table as it is no longer needed
        PreparedStatement dropTemp = db.prepareStatement("DROP TABLE tempalbum CASCADE");
        dropTemp.execute();
        dropTemp.close();
        addArtist.close();
        addTag.close();
        addTempAlbum.close();
        addAlbum.close();
    }
    /** secondPass is a method that iterates through the arraylist of results created after
     * reading in the CSV file and then uses the data contained within to populate the
     * song, and songtag tables of the database.
     *
     * @throws SQLException
     */
    public void secondPass() throws SQLException {
        String songName, albumName, artistName;

        PreparedStatement addSong = db.prepareStatement("INSERT INTO song (name, albumid, artistid) " +
                "VALUES (?, " +
                "(SELECT id FROM album WHERE album.name = ? AND album.artistid = (SELECT id FROM artist WHERE artist.name = ?))," +
                "(SELECT id FROM artist WHERE name = ?))");

        PreparedStatement addSongtag = db.prepareStatement("INSERT INTO songtag(songid, tagid) VALUES (" +
                "(SELECT song.id FROM song JOIN album ON song.albumid = album.id WHERE song.name = ? AND album.name = ?), " +
                "(SELECT tag.id FROM tag WHERE tag.name = ?))");

        for (String[] s : results) {
            songName = s[0];
            albumName = s[2];
            artistName = s[1];

            // set variables for PreparedStatement addSong
            addSong.setString(1, songName);
            addSong.setString(2, albumName);
            addSong.setString(3, artistName);
            addSong.setString(4, artistName);

            // Execute the PreparedStatement
            addSong.execute();

            for (int i = 3; i < s.length; i++) {
                if (s.length >= 4 && s[i].length() != 0) {
                    addSongtag.setString(1, songName);
                    addSongtag.setString(2, albumName);
                    addSongtag.setString(3, s[i]);
                    addSongtag.execute();
                }
            }
        }
        addSong.close();
        addSongtag.close();
    }

    /** dropTables is a method which does pretty much what it says on the tin.
     * It drops all of the tables in the database if they exist.
     *
     * @throws SQLException
     */
    public void dropTables() throws SQLException {
        PreparedStatement dropEverything = db.prepareStatement("DROP TABLE artist CASCADE;" +
                "DROP TABLE IF EXISTS album CASCADE;" +
                "DROP TABLE IF EXISTS tempalbum CASCADE;" +
                "DROP TABLE IF EXISTS song CASCADE;" +
                "DROP TABLE IF EXISTS tag CASCADE;" +
                "DROP TABLE IF EXISTS songtag CASCADE;");
        dropEverything.execute();
        dropEverything.close();
    }

    /** queries is a method which answers the 6 questions outlined in the assessment and prints
     * both the question and the answer to the console.
     *
     * @throws SQLException
     */
    public void queries() throws SQLException {
        System.out.print("How many albums are listed?\n");
        Statement st = db.createStatement();
        ResultSet rs = st.executeQuery("SELECT count(id) FROM album");
        while (rs.next()) {
            System.out.print(rs.getString(1));
        }
        System.out.println("\n");

        System.out.print("How many albums are classic rock?\n");
        rs = st.executeQuery("SELECT DISTINCT count(album.name) FROM album " +
                "JOIN song ON album.id = song.albumid " +
                "JOIN songtag ON song.id = songtag.songid " +
                "JOIN tag on tag.id = songtag.tagid " +
                "WHERE tag.name = 'classic rock';");
        while (rs.next()) {
            System.out.print(rs.getString(1));
        }
        System.out.println("\n");

        System.out.println("List in ALPHABETICAL order all the artists who have tracks regarded as 'rhythmic':");
        rs = st.executeQuery("SELECT DISTINCT artist.name FROM artist " +
                "JOIN song ON artist.id = song.artistid " +
                "JOIN songtag ON song.id = songtag.songid " +
                "JOIN tag on songtag.tagid = tag.id " +
                "WHERE tag.name = 'rhythmic' ORDER BY artist.name;");
        while (rs.next()) {
            if (rs.isLast()) {
                System.out.print(rs.getString(1) + ".");
            } else {
                System.out.print(rs.getString(1) + ", ");
            }
        }
        System.out.println("\n");

        System.out.println("What is the NUMERICAL difference between songs with 'love' in the title and those that are tagged with 'love' that DON'T have it in the title? ");
        rs = st.executeQuery("SELECT abs((SELECT count(song.id) FROM song " +
                "JOIN songtag ON songtag.songid = song.id " +
                "WHERE songtag.tagid = (SELECT id FROM tag WHERE name = 'love') " +
                "AND song.name NOT LIKE '%LOVE%') " +
                "- (SELECT count(song.name) FROM song WHERE song.name LIKE '%LOVE%'));");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        System.out.println("\n");

        System.out.println("How many albums have at least one song with 'DANCE' in the title?");
        rs = st.executeQuery("SELECT count(album.name) FROM album " +
                "JOIN song ON album.id = song.albumid " +
                "WHERE song.name LIKE '%DANCE%';");
        while (rs.next()) {
            System.out.print(rs.getString(1));
        }
        System.out.println("\n");

        System.out.println("Are there more playful songs than rhythmic ones?");
        rs = st.executeQuery("SELECT (SELECT count(songtag.id) FROM songtag " +
                "JOIN tag ON songtag.tagid = tag.id WHERE tag.name = 'playful') " +
                "> (SELECT count(songtag.id) FROM songtag " +
                "JOIN tag ON songtag.tagid = tag.id WHERE tag.name = 'rhythmic');");

        while (rs.next()) {
            System.out.println(rs.getBoolean(1));
        }
        System.out.println("\n");

        st.close();
        rs.close();
    }

    public static void main(String[] args) {
        try {
            Databases test = new Databases("mod-fund-databases.cs.bham.ac.uk/rkc604", "rkc604", "obvious");
            test.readCSV("artists-songs-albums-tags.csv");
            test.startConnection();

//            If running multiple queries comment out the following...
            test.dropTables();
            test.createTables();
            test.cleanTables();
            test.firstPass();
            test.secondPass();
//            Up to here. Otherwise you'll rebuild the database every time

            test.queries();
            test.closeConnection();
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
