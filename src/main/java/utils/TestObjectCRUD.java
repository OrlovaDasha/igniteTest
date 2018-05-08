package utils;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import domain.TestObject;
import org.bson.Document;

public class TestObjectCRUD {

    private MongoCollection table;

    public MongoCollection getTable() {
        return table;
    }

    public void setTable(MongoCollection table) {
        this.table = table;
    }

    public void add(TestObject testObject){
        BasicDBObject document = new BasicDBObject();
        document.put("id", testObject.getId());
        document.put("name", testObject.getName());
        document.put("description", testObject.getDescription());
        table.insertOne(document);
    }

    public TestObject getById(Long id){
        BasicDBObject query = new BasicDBObject();
        query.put("id", id);
        FindIterable<Document> resultSet = table.find(query);
        Document result = resultSet.first();
        TestObject testObject = new TestObject();
        testObject.setId(Long.valueOf(String.valueOf(result.get("id"))));
        testObject.setName(String.valueOf(result.get("name")));
        testObject.setDescription(String.valueOf(result.get("description")));
        return testObject;
    }

    public static TestObject fromDocument(Document document) {
        TestObject testObject = new TestObject();
        testObject.setId(Long.valueOf(String.valueOf(document.get("id"))));
        testObject.setName(String.valueOf(document.get("name")));
        testObject.setDescription(String.valueOf(document.get("description")));
        return testObject;
    }
}
