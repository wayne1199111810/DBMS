package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;

public class MyFakebookOracle extends FakebookOracle {

    static String prefix = "syzhao.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding tables in your database
    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;


    // DO NOT modify this constructor
    public MyFakebookOracle(String dataType, Connection c) {
        super();
        oracleConnection = c;
        // You will use the following tables in your Java code
        cityTableName = prefix + dataType + "_CITIES";
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITY";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITY";
        programTableName = prefix + dataType + "_PROGRAMS";
        educationTableName = prefix + dataType + "_EDUCATION";
        eventTableName = prefix + dataType + "_USER_EVENTS";
        albumTableName = prefix + dataType + "_ALBUMS";
        photoTableName = prefix + dataType + "_PHOTOS";
        tagTableName = prefix + dataType + "_TAGS";
    }


    @Override
    // ***** Query 0 *****
    // This query is given to your for free;
    // You can use it as an example to help you write your own code
    //
    public void findMonthOfBirthInfo() {

        // Scrollable result set allows us to read forward (using next())
        // and also backward.
        // This is needed here to support the user of isFirst() and isLast() methods,
        // but in many cases you will not need it.
        // To create a "normal" (unscrollable) statement, you would simply call
        // Statement stmt = oracleConnection.createStatement();
        //
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {

            // For each month, find the number of users born that month
            // Sort them in descending order of count
            ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from " +
                    userTableName +
                    " where month_of_birth is not null group by month_of_birth order by 1 desc");

            this.monthOfMostUsers = 0;
            this.monthOfLeastUsers = 0;
            this.totalUsersWithMonthOfBirth = 0;

            // Get the month with most users, and the month with least users.
            // (Notice that this only considers months for which the number of users is > 0)
            // Also, count how many total users have listed month of birth (i.e., month_of_birth not null)
            //
            while (rst.next()) {
                int count = rst.getInt(1);
                int month = rst.getInt(2);
                if (rst.isFirst())
                    this.monthOfMostUsers = month;
                if (rst.isLast())
                    this.monthOfLeastUsers = month;
                this.totalUsersWithMonthOfBirth += count;
            }

            // Get the names of users born in the "most" month
            rst = stmt.executeQuery("select user_id, first_name, last_name from " +
                    userTableName + " where month_of_birth=" + this.monthOfMostUsers);
            while (rst.next()) {
                Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                this.usersInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
            }

            // Get the names of users born in the "least" month
            rst = stmt.executeQuery("select first_name, last_name, user_id from " +
                    userTableName + " where month_of_birth=" + this.monthOfLeastUsers);
            while (rst.next()) {
                String firstName = rst.getString(1);
                String lastName = rst.getString(2);
                Long uid = rst.getLong(3);
                this.usersInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
            }

            // Close statement and result set
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 1 *****
    // Find information about users' names:
    // (1) The longest first name (if there is a tie, include all in result)
    // (2) The shortest first name (if there is a tie, include all in result)
    // (3) The most common first name, and the number of times it appears (if there
    //      is a tie, include all in result)
    //
    public void findNameInfo() { // Query1
        // Find the following information from your database and store the information as shown
        /*
        this.longestFirstNames.add("JohnJacobJingleheimerSchmidt");         
        this.shortestFirstNames.add("Al");         
        this.shortestFirstNames.add("Jo");         
        this.shortestFirstNames.add("Bo");         
        this.mostCommonFirstNames.add("John");         
        this.mostCommonFirstNames.add("Jane");
        this.mostCommonFirstNamesCount = 10;
        */
        try(Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)){
            ResultSet rst = stmt.executeQuery("select count(*), first_name from " + userTableName + 
                " where first_name is not null group by first_name order by 1 desc");
            this.mostCommonFirstNamesCount = 0;
            int longest = 0;
            int shortest = Integer.MAX_VALUE;
            while(rst.next())
            {
                if(longest < rst.getString(2).length())
                {
                    this.longestFirstNames = new TreeSet<String>();
                    this.longestFirstNames.add(rst.getString(2));
                    longest = rst.getString(2).length();
                }
                else if (longest == rst.getString(2).length())
                {
                    this.longestFirstNames.add(rst.getString(2));
                }
                if(shortest > rst.getString(2).length())
                {
                    this.shortestFirstNames = new TreeSet<String>();
                    this.shortestFirstNames.add(rst.getString(2));
                    shortest = rst.getString(2).length();
                }
                else if (shortest == rst.getString(2).length())
                {
                    this.shortestFirstNames.add(rst.getString(2));
                }
                if(rst.isFirst())
                {
                    this.mostCommonFirstNamesCount = rst.getInt(1);
                    this.mostCommonFirstNames.add(rst.getString(2));

                }
                else
                {
                    if(rst.getInt(1) == this.mostCommonFirstNamesCount)
                    {
                        this.mostCommonFirstNames.add(rst.getString(2));
                    }
                }
            }
            rst.close();
            stmt.close();
        } catch(SQLException err){
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 2 *****
    // Find the user(s) who have no friends in the network
    //
    // Be careful on this query!
    // Remember that if two users are friends, the friends table
    // only contains the pair of user ids once, subject to
    // the constraint that user1_id < user2_id
    //
    public void lonelyUsers() {
        // Find the following information from your database and store the information as shown
        /*
        this.lonelyUsers.add(new UserInfo(10L, "Billy", "SmellsFunny"));         
        this.lonelyUsers.add(new UserInfo(11L, "Jenny", "BadBreath"));
        */
        try(Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)){
            ResultSet rst = stmt.executeQuery("select U.user_id, U.first_name, U.last_name from " + userTableName +
                " U where U.user_id not in (select user1_id as user_id from " + friendsTableName + 
                " union select user2_id as user_id from " + friendsTableName + ") order by U.user_id");
            while (rst.next()) {
                Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                this.lonelyUsers.add(new UserInfo(uid, firstName, lastName));
            }
            rst.close();
            stmt.close();
        }catch(SQLException err){
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 3 *****
    // Find the users who do not live in their hometowns
    // (I.e., current_city != hometown_city)
    //
    public void liveAwayFromHome() throws SQLException {
        try(Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)){
            ResultSet rst = stmt.executeQuery("select U.user_id, U.first_name, U.last_name from " + userTableName +
                " U, " + hometownCityTableName + " H, " + currentCityTableName + " C where U.user_id = H.user_id and " +
                " U.user_id = C.user_id and H.hometown_city_id <> C.current_city_id order by U.user_id");
            while(rst.next())
            {
                Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                this.liveAwayFromHome.add(new UserInfo(uid, firstName, lastName));
            }
            rst.close();
            stmt.close();
        }catch(SQLException err){
            System.err.println(err.getMessage());
        }
    }

    @Override
    // **** Query 4 ****
    // Find the top-n photos based on the number of tagged users
    // If there are ties, choose the photo with the smaller numeric PhotoID first
    //
    public void findPhotosWithMostTags(int n) {
        /*
        String photoId = "1234567";
        String albumId = "123456789";
        String albumName = "album1";
        String photoCaption = "caption1";
        String photoLink = "http://google.com";
        PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);
        TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
        tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName1", "taggedUserLastName1"));
        tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName2", "taggedUserLastName2"));
        this.photosWithMostTags.add(tp);
*/
        try(Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)){
            ResultSet rst = stmt.executeQuery("select count(*), T.tag_photo_id, A.album_id, A.album_name, P.photo_caption, " +
                "P.photo_link from " + tagTableName + " T, " + photoTableName + " P,"+ albumTableName +
                " A where P.photo_id = T.tag_photo_id and P.album_id = A.album_id" +
                " group by T.tag_photo_id order by 1 desc, T.tag_photo_id asc limit " + n);
            while(rst.next())
            {
                String photoId = rst.getString(2);
                String albumId = rst.getString(3);
                String albumName = rst.getString(4);
                String photoCaption = rst.getString(5);
                String photoLink = rst.getString(6);
                PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                ResultSet rst2 = stmt.executeQuery("select U.user_id, U.first_name, u.last_name from " +
                    userTableName + " U, " + tagTableName + " T where U.user_id = T.tag_subject_id " +
                    "and T.tag_photo_id = " + photoId);
                while(rst2.next())
                {
                    Long uid = rst.getLong(1);
                    String firstName = rst.getString(2);
                    String lastName = rst.getString(3);
                    tp.addTaggedUser(new UserInfo(uid, firstName, lastName));
                }
                rst2.close();
                this.photosWithMostTags.add(tp);
            }
            rst.close();
            stmt.close();
        }catch(SQLException err){
            System.err.println(err.getMessage());
        }
    }

    @Override
    // **** Query 5 ****
    // Find suggested "match pairs" of users, using the following criteria:
    // (1) Both users should be of the same gender
    // (2) They should be tagged together in at least one photo (They do not have to be friends of the same person)
    // (3) Their age difference is <= yearDiff (just compare the years of birth for this)
    // (4) They are not friends with one another
    //
    // You should return up to n "match pairs"
    // If there are more than n match pairs, you should break ties as follows:
    // (i) First choose the pairs with the largest number of shared photos
    // (ii) If there are still ties, choose the pair with the smaller user1_id
    // (iii) If there are still ties, choose the pair with the smaller user2_id
    //
    public void matchMaker(int n, int yearDiff) {
        /*
        Long u1UserId = 123L;
        String u1FirstName = "u1FirstName";
        String u1LastName = "u1LastName";
        int u1Year = 1988;
        Long u2UserId = 456L;
        String u2FirstName = "u2FirstName";
        String u2LastName = "u2LastName";
        int u2Year = 1986;
        MatchPair mp = new MatchPair(u1UserId, u1FirstName, u1LastName,
                u1Year, u2UserId, u2FirstName, u2LastName, u2Year);
        String sharedPhotoId = "12345678";
        String sharedPhotoAlbumId = "123456789";
        String sharedPhotoAlbumName = "albumName";
        String sharedPhotoCaption = "caption";
        String sharedPhotoLink = "link";
        mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId,
                sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
        this.bestMatches.add(mp);
        */
        try(Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)){
            ResultSet rst = stmt.executeQuery(
                "select count(*), U1.user_id, U2.user_id from " + userTableName + " U1, " + userTableName +
                " U2, " + tagTableName + " T1," + tagTableName + " T2 where U1.gender = U2.gender and " +
                " U2.year_of_birth - U1.year_of_birth <= " + yearDiff +" and U2.user_id > U1.user_id " +
                "and U1.user_id = T1.tag_subject_id and U2.user_id = T2.user_id group by T1."
                );


            while(rst.next())
            {

            }
            rst.close();
            stmt.close();
        }catch(SQLException err){
            System.err.println(err.getMessage());
        }
    }

    // **** Query 6 ****
    // Suggest users based on mutual friends
    //
    // Find the top n pairs of users in the database who have the most
    // common friends, but are not friends themselves.
    //
    // Your output will consist of a set of pairs (user1_id, user2_id)
    // No pair should appear in the result twice; you should always order the pairs so that
    // user1_id < user2_id
    //
    // If there are ties, you should give priority to the pair with the smaller user1_id.
    // If there are still ties, give priority to the pair with the smaller user2_id.
    //
    @Override
    public void suggestFriendsByMutualFriends(int n) {
        Long user1_id = 123L;
        String user1FirstName = "User1FirstName";
        String user1LastName = "User1LastName";
        Long user2_id = 456L;
        String user2FirstName = "User2FirstName";
        String user2LastName = "User2LastName";
        UsersPair p = new UsersPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);

        p.addSharedFriend(567L, "sharedFriend1FirstName", "sharedFriend1LastName");
        p.addSharedFriend(678L, "sharedFriend2FirstName", "sharedFriend2LastName");
        p.addSharedFriend(789L, "sharedFriend3FirstName", "sharedFriend3LastName");
        this.suggestedUsersPairs.add(p);
        try(Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)){
            ResultSet rst = stmt.executeQuery(
                "select U1.user_id, U1.first_name, U1.last_name, U2.user_id, U2.first_name, " +
                "U2.last_name + from " + userTableName + " U1, " + userTableName + " U2, " +
                friendsTableName + " F1, "
                );
            while(rst.next())
            {

            }
            rst.close();
            stmt.close();
        }catch(SQLException err){
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 7 *****
    //
    // Find the name of the state with the most events, as well as the number of
    // events in that state.  If there is a tie, return the names of all of the (tied) states.
    //
    public void findEventStates() {
        this.eventCount = 12;
        this.popularStateNames.add("Michigan");
        this.popularStateNames.add("California");
        try(Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)){
            ResultSet rst = stmt.executeQuery(
                "select count(*) C.state_name from " + eventTableName + " E, " + cityTableName +
                 "C where C.city_id = E.event_city_id group by C.city_id order by 1 desc");
            while(rst.next())
            {
                if(rst.isFirst())
                {
                    this.eventCount = rst.getInt(1);
                    String stateName = rst.getString(2);
                    this.popularStateNames.add(stateName);
                }
                else
                {
                    if(this.eventCount == rst.getInt(1))
                    {
                        String stateName = rst.getString(2);
                        this.popularStateNames.add(stateName);
                    }   
                    else    
                        break;  
                }   
            }
            rst.close();
            stmt.close();
        }catch(SQLException err){
            System.err.println(err.getMessage());
        }
    }

    //@Override
    // ***** Query 8 *****
    // Given the ID of a user, find information about that
    // user's oldest friend and youngest friend
    //
    // If two users have exactly the same age, meaning that they were born
    // on the same day, then assume that the one with the larger user_id is older
    //
    public void findAgeInfo(Long user_id) {
        /*
        this.oldestFriend = new UserInfo(1L, "Oliver", "Oldham");
        this.youngestFriend = new UserInfo(25L, "Yolanda", "Young");
        */
        try(Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)){
            ResultSet rst = stmt.executeQuery(
                "select U.user_id, U.first_name, U.last_name from " + userTableName + " U, " + 
                "(select U.user_id as fuid from " + userTableName + " U," + friendsTableName +
                " F where U.user_id = F.user1_id and F.user2_id = " + user_id + " union " +
                "select U.user_id as fuid from " + userTableName + " U," + friendsTableName +
                " F where U.user_id = F.user2_id and F.user1_id = " + user_id + ") F where " +
                "F.fuid = U.user_id order by U.year_of_birth asc, U.month_of_birth asc, " +
                "U.day_of_birth asc, U.user_id"
                );
            while(rst.next())
            {
                if(rst.isFirst())
                {
                    Long uid = rst.getLong(1);
                    String firstName = rst.getString(2);
                    String lastName = rst.getString(3);
                    this.oldestFriend = new UserInfo(uid, firstName, lastName);
                }
                if(rst.isLast())
                {
                    Long uid = rst.getLong(1);
                    String firstName = rst.getString(2);
                    String lastName = rst.getString(3);
                    this.youngestFriend = new UserInfo(uid, firstName, lastName);
                }
            }
            rst.close();
            stmt.close();
        }catch(SQLException err){
            System.err.println(err.getMessage());
        }
    }

    @Override
    //	 ***** Query 9 *****
    //
    // Find pairs of potential siblings.
    //
    // A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
    // if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
    // on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
    //
    //
    public void findPotentialSiblings() {
        /*
        Long user1_id = 123L;
        String user1FirstName = "User1FirstName";
        String user1LastName = "User1LastName";
        Long user2_id = 456L;
        String user2FirstName = "User2FirstName";
        String user2LastName = "User2LastName";
        SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
        this.siblings.add(s);
        */
        try(Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)){
            ResultSet rst = stmt.executeQuery(
                "select U1.user_id, U1.first_name, U1.last_name, U2.user_id, U2.first_name, U2.last_name " +
                "from " + userTableName + " U1, " + userTableName + " U2, " + friendsTableName + " F, " +
                hometownCityTableName + " H1, " + hometownCityTableName + " H2 where " +
                "F.user1_id = U1.user_id and F.user2_id = U2.user_id and U1.user_id = H1.user_id " +
                "and U2.user_id = H2.user_id and H1.hometown_city_id = H2.hometown_city_id and " +
                "U1.year_of_birth - U2.year_of_birth < 10 and U1.year_of_birth - U2.year_of_birth > -10 " +
                "and U1.user_id < U2.user_id order by U1.user_id, U2.user_id"
                );
            while(rst.next())
            {
                Long user1_id = rst.getLong(1);
                String user1FirstName = rst.getString(2);
                String user1LastName = rst.getString(3);
                Long user2_id = rst.getLong(4);
                String user2FirstName = rst.getString(5);
                String user2LastName = rst.getString(6);
                SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
                this.siblings.add(s);
            }
            rst.close();
            stmt.close();
        }catch(SQLException err){
            System.err.println(err.getMessage());
        }
    }
}