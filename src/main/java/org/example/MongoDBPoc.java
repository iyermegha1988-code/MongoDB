package org.example;

import com.mongodb.client.*;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import java.util.ArrayList;
import java.util.Arrays;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class MongoDBPoc {
    static MongoClient mongoClient;
    static MongoDatabase mongoDatabase;
    static MongoCollection<Document> mongoCollection;

    public static void main(String[] args) {

        try {
            mongoClient = MongoClients.create("mongodb://localhost:27017");
            mongoDatabase = mongoClient.getDatabase("mongo_db");
            mongoCollection = mongoDatabase.getCollection("mongo" + System.currentTimeMillis());

            Document doc1 = new Document("name", "megha").append("age", 35).append("role", "Senior QA");
            Document doc2 = new Document("name", "alok").append("age", 40).append("role", "Lead QA");
            Document doc3 = new Document("name", "arnav").append("age", 43).append("role", "Scrum master");
            Document doc4 = new Document("name", "pankaj").append("age", 37).append("role", "Lead QA");

            ArrayList<Document> listValue = new ArrayList<Document>(Arrays.asList(doc1, doc2, doc3, doc4));
            //calling function for insertion
            insertValues(listValue);
            //calling function for updating
            updateValues();
            //calling function for sorting/projection
            sortValues();
            //calling function for deleting
            deleteRecords();
            //calling function for aggregate function - count/sum/avg
            aggregateFlows();
        } catch (Exception e) {
            System.out.println("Exception in mongo DB operation flows");
        }
        mongoClient.close();
    }

    public static void insertValues(ArrayList<Document> listValue) {
        mongoCollection.insertMany(listValue);

        //fetch the first record details
        System.out.println("**fetch the first record details**");
        Document details = mongoCollection.find().first();
        System.out.println(details.toJson());

        //To iterate through all the values in the collection
        Document doc5 = new Document("name", "Naveen").append("age", 23).append("role", "Project Coordinator");
        mongoCollection.insertOne(doc5);

        System.out.println("**To iterate through all the values in the collection**");
        FindIterable<Document> findList = mongoCollection.find();
        for (Document value : findList) {
            System.out.println(value.toJson());
        }
    }

    public static void updateValues() {
        //Update any 1 value in the collection
        mongoCollection.updateOne(eq("name", "arnav"), Updates.set("age", 67));
        //Printing the updated details in the collection
        System.out.println("**Printing the updated details in the collection**");
        Document updatedDetails = mongoCollection.find(eq("name", "arnav")).first();
        System.out.println(updatedDetails.toJson());

        //Update many values in the collection
        mongoCollection.updateMany(regex("role", "QA"), Updates.set("role", "Quality Assurance"));
        //To iterate through all the values in the collection
        System.out.println("**To iterate through all the updated values in the collection**");
        FindIterable<Document> findListUpdated = mongoCollection.find();
        for (Document value : findListUpdated) {
            System.out.println(value.toJson());
        }
    }

    public static void sortValues() {
        System.out.println("****Sort age by desc");
        FindIterable<Document> sortDetails = mongoCollection.find().sort(descending("age"));
        for (Document value : sortDetails) {
            System.out.println(value.toJson());
        }
        System.out.println("Fetching only name and role and sort age by asc");
        FindIterable<Document> sortProjectedDetails = mongoCollection.find().projection(include("name")).sort(ascending("age"));
        for (Document value : sortProjectedDetails) {
            System.out.println(value.toJson());
        }
    }

    public static void deleteRecords() {
        System.out.println("Deleting a record");
        DeleteResult result = mongoCollection.deleteMany(regex("role", "Project"));

        System.out.println(result.getDeletedCount() + " document(s) deleted.");

        Document deletedRecord = mongoCollection.find(regex("role", "Project")).first();
        if (deletedRecord == null) {
            System.out.println("Role containing Project is removed");
        } else {
            System.out.println("Role containing Project is not removed");
        }
        System.out.println("After deleting the records in collection");
        FindIterable<Document> updatedDeletedDetails = mongoCollection.find();
        for (Document value : updatedDeletedDetails) {
            System.out.println(value.toJson());
        }
    }

    public static void aggregateFlows() {
        System.out.println("Count up the values in collection");
        mongoCollection.aggregate(Arrays.asList(
                        new Document("$group", new Document("_id", "$role").
                                append("count", new Document("$sum", 1))))).
                forEach(doc -> System.out.println(doc.toJson()));

        System.out.println("Average age of all values in collection");
        mongoCollection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$role").append(
                        "avg", new Document("$avg", "$age"))))).forEach(doc -> System.out.println(doc.toJson()));

        System.out.println("Sum age of all values in collection");
        mongoCollection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$role").append(
                        "sum", new Document("$sum", "$age"))))).forEach(doc -> System.out.println(doc.toJson()));
    }

}