package be.datablend.datomic;

import datomic.Peer;
import datomic.Util;
import datomic.Connection;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * User: dsuvee
 * Date: 12/04/12
 */
public class Test {

    private Connection conn;
    private Date checkpoint;

    public void setup() throws ExecutionException, InterruptedException, FileNotFoundException {
        // Setup connection
        String uri = "datomic:mem://test";
        Peer.createDatabase(uri);
        conn = Peer.connect(uri);

        // Define attribute schema
        List tx = new ArrayList();

        // Add new attributes
        tx.add(Util.map(":db/id", Peer.tempid(":db.part/db"),
                        ":db/ident", ":person/name",
                        ":db/valueType", ":db.type/string",
                        ":db/cardinality", ":db.cardinality/one",
                        ":db/doc", "A person's name",
                        ":db.install/_attribute", ":db.part/db"));

        tx.add(Util.map(":db/id", Peer.tempid(":db.part/db"),
                        ":db/ident", ":person/parent",
                        ":db/valueType", ":db.type/ref",
                        ":db/cardinality", ":db.cardinality/many",
                        ":db/doc", "A person's parent",
                        ":db.install/_attribute", ":db.part/db"));

        // Store the schema
        conn.transact(tx).get();
    }

    public void createData() throws ExecutionException, InterruptedException {
        // Define person entities
        List tx = new ArrayList();

        Object edmond = Peer.tempid(":db.part/user");
        tx.add(Util.map(":db/id", edmond,
                        ":person/name", "Edmond Suvee"));

        Object gilbert = Peer.tempid(":db.part/user");
        tx.add(Util.map(":db/id", gilbert,
                        ":person/name", "Gilbert Suvee",
                        ":person/parent", edmond));

        // Persist
        conn.transact(tx).get();

        // Create checkpoint
        Thread.sleep(1000);
        checkpoint = Calendar.getInstance().getTime();
        Thread.sleep(1000);

        // Add person Davy Suvee
        tx.clear();
        Object davy = Peer.tempid(":db.part/user");
        tx.add(Util.map(":db/id", davy,
                        ":person/name", "Davy Suvee",
                        ":person/parent", Peer.q("[:find ?person :where [?person :person/name \"Gilbert Suvee\"] ]", conn.db()).iterator().next().get(0)));

        // Persist
        conn.transact(tx).get();

    }

    public void query() {
        System.out.println(Peer.q("[:find ?name " +
                                   ":where [?person :person/name ?name] ]", conn.db()));

        System.out.println(Peer.q("[:find ?name ?parentname " +
                                   ":where [?person :person/name ?name] " +
                                          "[?person :person/parent ?parent] " +
                                          "[?parent :person/name ?parentname] ]"  , conn.db()));

        System.out.println(Peer.q("[:find ?name ?grandparentname " +
                                   ":where [?person :person/name ?name] " +
                                          "[?person :person/parent ?parent] " +
                                          "[?parent :person/parent ?grandparent] " +
                                          "[?grandparent :person/name ?grandparentname] ]"  , conn.db()));

        String grandparentrule = "[ [ (grandparent ?person ?grandparent) [?person :person/parent ?parent] " +
                                                                        "[?parent :person/parent ?grandparent] ] ]";

        System.out.println(Peer.q("[:find ?name ?grandparentname " +
                                   ":in $ % " +
                                   ":where [?person :person/name ?name] " +
                                                  "(grandparent ?person ?grandparent) " +
                                                  "[?grandparent :person/name ?grandparentname] ]"  , conn.db(), grandparentrule));

        String ancestorrule = "[ [ (ancestor ?person ?ancestor) [?person :person/parent ?ancestor] ] " +
                              "[ (ancestor ?person ?ancestor) [?person :person/parent ?parent] " +
                                                             "(ancestor ?parent ?ancestor) ] ] ]";

        System.out.println(Peer.q("[:find ?name ?ancestorname " +
                                   ":in $ % " +
                                   ":where [?person :person/name ?name] " +
                                          "[ancestor ?person ?ancestor] " +
                                          "[?ancestor :person/name ?ancestorname] ]"  , conn.db(), ancestorrule));

        System.out.println(Peer.q("[:find ?name ?parentname " +
                                   ":where [?person :person/name ?name] " +
                                          "[?person :person/parent ?parent] " +
                                          "[?parent :person/name ?parentname] ]", conn.db().asOf(getCheckPoint(checkpoint))));


        List tx = new ArrayList();
        tx.add(Util.map(":db/id", Peer.tempid(":db.part/user"),
                        ":person/name", "FutureChild Suvee",
                        ":person/parent", Peer.q("[:find ?person :where [?person :person/name \"Davy Suvee\"] ]", conn.db()).iterator().next().get(0)));

        System.out.println(Peer.q("[:find ?name ?ancestorname " +
                                   ":in $ % " +
                                   ":where [?person :person/name ?name] " +
                                          "[ancestor ?person ?ancestor] " +
                                          "[?ancestor :person/name ?ancestorname] ]"  , conn.db().with(tx), ancestorrule));



    }

     private long getCheckPoint(Date checkpoint) {
        Long transaction = 0L;
        // Retrieve the transactions
        Iterator<List<Object>> tx = (Peer.q("[:find ?tx ?when " +
                                            ":where [?tx :db/txInstant ?when]]", conn.db())).iterator();
        while (tx.hasNext()) {
            List<Object> txobject = tx.next();
            Long transactionid = (Long)txobject.get(0);
            Date time = (Date)txobject.get(1);
            if (time.getTime() <= checkpoint.getTime()) {
                if (transaction == null) {
                    transaction = transactionid;
                }
                else {
                    if (transactionid > transaction) {
                        transaction = transactionid;
                    }
                }
            }
        }
        return transaction;
    }

    public static void main(String[] args) throws FileNotFoundException, ExecutionException, InterruptedException {
        Test test = new Test();
        test.setup();
        test.createData();
        test.query();
    }

}
