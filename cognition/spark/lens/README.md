#Lens

Lens is the backend engine fusing spark and accumulo into a query engine.

##Criteria
The criteria object holds and maps all query parameters. See the README.md file within the lens-endpoint folder for more information about specifying the different criteria.

##SchemaAdapter
Instead of directly using the accumulo column names to specify our criteria, we use a higher level specification to break
the logic away from the reliance on the back end datastore (accumulo). This allows us to easily change the schema and 
ensures everything still works correctly.
