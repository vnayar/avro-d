module avro.codec.datumwriter;

import avro.schema;

@safe:

/**
   Write data of a schema.

   Implemented for in-memory data representations.
*/
interface DatumWriter(D) {
  /// Sets the schema used to encode data.
  void setSchema(Schema schema);

  /**
     Write the datum. Traverse the schema, depth first, writing each leave value in the schema from
     the datum to the encoder.
  */
  void write(D datum, Encoder encoder);
}
