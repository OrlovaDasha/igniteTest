import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
        document.put("id", testObject.id);
        document.put("name", testObject.name);
        document.put("description", testObject.description);
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

    public Map<Long, TestObject> getAll(){
        Map<Long, TestObject> testObjectMap = new HashMap<>();
        FindIterable<Document> cursor = table.find();
        Iterator it = cursor.iterator();

        while (it.hasNext()) {
            System.out.println(it.next());
            DBObject dbObject = (DBObject) it.next();
            TestObject testObject = new TestObject();
            testObject.setId(Long.valueOf(String.valueOf(dbObject.get("id"))));
            testObject.setName(String.valueOf(dbObject.get("name")));
            testObject.setDescription(String.valueOf(dbObject.get("description")));
            testObjectMap.put(testObject.id, testObject);
        }
        return testObjectMap;
    }

    public static TestObject fromDocument(Document document) {
        TestObject testObject = new TestObject();
        testObject.setId(Long.valueOf(String.valueOf(document.get("id"))));
        testObject.setName(String.valueOf(document.get("name")));
        testObject.setDescription(String.valueOf(document.get("description")));
        return testObject;
    }
}
