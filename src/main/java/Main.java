import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.neo4j.graphdb.Label.label;

public class Main
{
    private static final Pattern DESCRIPTION_PATTERN_UNIQUE_CONSTRAINT = Pattern.compile( "^CONSTRAINT ON \\( .+:(.+) \\) ASSERT .+\\.(.+) IS UNIQUE$" );
    private static final Pattern DESCRIPTION_PATTERN_LABEL_PROPERTY_INDEX = Pattern.compile( "^INDEX ON :(.+)\\((.+)\\)$" );

    public static void main(String ... argv) throws IOException
    {
        BatchInserter inserter = BatchInserters.inserter( new File( "./import" ) );

        try {
            Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "asd" ) );

            try ( Session session = driver.session() )
            {
                importNodes( inserter, session );
                importRelationships( inserter, session );
                importIndexes( inserter, session );
                importConstraints( inserter, session );
            }
            finally
            {
                driver.close();
            }

        } finally {
            inserter.shutdown();
        }
    }

    private static void importNodes( BatchInserter inserter, Session session )
    {
        System.out.print("[Import] Importing nodes");
        long n = 10_000;
        StatementResult rs = session.run( "MATCH (n) RETURN n" );

        while(rs.hasNext()) {
            Node node = rs.next().get( 0 ).asNode();

            List<Label> labels = new ArrayList<>();
            node.labels().forEach( l -> labels.add(label(l)) );

            Map<String,Object> properties = node.asMap( toPrimitivePropertyType );
            inserter.createNode(
                    node.id(),
                    properties,
                    labels.toArray( new Label[0] ) );

            n++;
            if((n % 10_000) == 0) {
                System.out.print(".");
            }
            if((n % 200_000) == 0) {
                System.out.print("\n         .");
            }
        }
        System.out.println();
    }

    private static void importRelationships( BatchInserter inserter, Session session )
    {
        System.out.print("[Import] Importing relationships");
        long n = 10_000;
        StatementResult rs = session.run( "MATCH ()-[r]->() RETURN r" );

        while(rs.hasNext()) {
            Relationship rel = rs.next().get( 0 ).asRelationship();

            inserter.createRelationship(
                    rel.startNodeId(),
                    rel.endNodeId(),
                    RelationshipType.withName( rel.type() ),
                    rel.asMap( toPrimitivePropertyType ) );

            n++;
            if((n % 10_000) == 0) {
                System.out.print(".");
            }
            if((n % 200_000) == 0) {
                System.out.print("\n         .");
            }
        }
        System.out.println();
    }

    private static void importConstraints( BatchInserter inserter, Session session )
    {
        System.out.println("[Import] Creating constraints..");
        StatementResult rs = session.run( "CALL db.constraints()" );
        while(rs.hasNext()) {
            Record index = rs.next();
            String description = index.get("description").asString();

            Matcher matcher = DESCRIPTION_PATTERN_UNIQUE_CONSTRAINT.matcher( description );
            if(matcher.matches()) {
                String label = matcher.group( 1 );
                String property = matcher.group( 2 );
                inserter.createDeferredConstraint( label(label) ).assertPropertyIsUnique( property ).create();
                continue;
            }

            throw new IllegalArgumentException(
                    String.format("Unknown constraint description, unable to import: `%s`", description) );
        }
    }

    private static void importIndexes( BatchInserter inserter, Session session )
    {
        System.out.println("[Import] Creating indexes..");
        StatementResult rs = session.run( "CALL db.indexes()" );
        while(rs.hasNext()) {
            Record index = rs.next();
            String description = index.get("description").asString();
            String type = index.get( "type" ).asString();

            if(type.equals( "node_label_property" )) {
                // Do the parsing very carefully, since the label name could contain lots of odd chars
                // Format: "INDEX ON :$LABELNAME($PROPNAME)"
                Matcher matcher = DESCRIPTION_PATTERN_LABEL_PROPERTY_INDEX.matcher( description );

                if( !matcher.matches() ||
                        StringUtils.countMatches( description, "(" ) != 1 ||
                        StringUtils.countMatches( description, ")" ) != 1) {
                    throw new IllegalArgumentException( String.format(
                            "Index is either on property/label with paren in the name, " +
                                    "or the description format of this index is unknown. Either way it can't be interpreted. " +
                                    "Offending index description: `%s`", description) );
                }

                String label = matcher.group(1);
                String property = matcher.group(2);
                inserter.createDeferredSchemaIndex( label(label) ).on( property ).create();
                continue;
            }

            if(type.equals( "node_unique_property" )) {
                // These get created when we pull in the constraints
                continue;
            }
        }
    }

    private static Function<Value,Object> toPrimitivePropertyType = v ->
    {
        Object javaValue = v.asObject();
        if ( !(javaValue instanceof List) )
        {
            return javaValue;
        }

        // Arrays need conversion to primitive java arrays
        List list = (List) javaValue;
        if ( list.size() == 0 )
        {
            return new int[0];
        }

        if ( list.get( 0 ) instanceof String ) // Note that Bolt does not have a char type
        {
            return list.toArray( new String[0] );
        }
        if ( list.get( 0 ) instanceof Long || list.get( 0 ) instanceof Integer )
        {
            return list.stream().mapToLong( l -> (long) l ).toArray();
        }
        if ( list.get( 0 ) instanceof Double || list.get( 0 ) instanceof Float )
        {
            return list.stream().mapToDouble( d -> (double) d ).toArray();
        }
        if ( list.get( 0 ) instanceof Boolean )
        {
            boolean[] out = new boolean[list.size()];
            for ( int i = 0; i < list.size(); i++ )
            {
                out[i] = (boolean) list.get( 0 );
            }
            return out;
        }

        throw new IllegalArgumentException(
                String.format( "Importer does not know how to handle array properties of type: %s", list.get( 0 ).getClass().getCanonicalName() ) );
    };
}
