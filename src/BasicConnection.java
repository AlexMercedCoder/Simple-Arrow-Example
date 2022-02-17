

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

      public FlightSqlClient createFlightSqlClient(final String host, final String port, final String user, final String pass) {
        final ClientIncomingAuthHeaderMiddleware.Factory factory =
            new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
        final FlightClient client = FlightClient.builder()
            .allocator(allocator)
            .location(Location.forGrpcInsecure(host, Integer.parseInt(port)))
            .intercept(factory)
            .build();
        client.handshake(new CredentialCallOption(new BasicAuthCredentialWriter(user, pass)));
        // add credential to options
        callOptions.add(factory.getCredentialCallOption());
        flightSqlClient = new FlightSqlClient(client);
        return flightSqlClient;
      }



    public static void main(final String[] args) throws Exception {


        String host = "localhost";
        String port = "32010";
        String user = "username";
        String pass = "password123";

        //Create Instance of class
        var app = new BasicConnection(new RootAllocator(Integer.MAX_VALUE));
        
        // get client
        var client = app.createFlightSqlClient(host, port, user, pass);

        // Prepare an SQL Statement
        var query = client.prepare("SELECT * FROM \"@username\".zips", app.getCallOptions());

        // execute query
        var results = query.execute();

        // get the schema of the results
        System.out.println(results.getSchema().toString());
        
    }
}
