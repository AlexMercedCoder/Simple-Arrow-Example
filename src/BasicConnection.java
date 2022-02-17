

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.example.FlightSqlClientDemoApp;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class BasicConnection extends FlightSqlClientDemoApp{
    
    // Constructor
    public BasicConnection(BufferAllocator bufferAllocator) {
        super(bufferAllocator);
      }

    // method for create FlightSQL Client
    public FlightSqlClient getClient(){
        // Database Settings
        String host = "localhost";
        int port = 32010;
        String user = "username";
        String pass = "password123";

        // Auth Middleware
        final ClientIncomingAuthHeaderMiddleware.Factory factory = new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());

        // Create Flight Client
        // Where we are connecting to and how
        final FlightClient client = FlightClient.builder().allocator(this.allocator).location(Location.forGrpcInsecure(host, port)).intercept(factory).build();

        //Client Handshake
        // Passing auth details to authenticate
        client.handshake(new CredentialCallOption(new BasicAuthCredentialWriter(user, pass)));

        // Create FlightSQL Client
        var flightSqlClient = new FlightSqlClient(client);

        return flightSqlClient;
    }

    public static void main(final String[] args) throws Exception {

        //Create Instance of class
        var app = new BasicConnection(new RootAllocator(Integer.MAX_VALUE));
        
        // get client
        var client = app.getClient();

        // Prepare an SQL Statement
        var query = client.prepare("SELECT * FROM '@alexmerced'.zips");

        // execute query
        var results = query.execute();

        // get the schema of the results
        results.getSchema();
        
    }
}
