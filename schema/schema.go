package schema

import (
	"github.com/ONSdigital/dp-kafka/v5/avro"
)

var imageUploadedEvent = `{
  "type": "record",
  "name": "image-uploaded",
  "fields": [
    {"name": "image_id", "type": "string", "default": ""},
    {"name": "path", "type": "string", "default": ""},
    {"name": "filename", "type": "string", "default": ""}
  ]
}`

var imagePublishedEvent = `{
  "type": "record",
  "name": "image-published",
  "fields": [
    {"name": "src_path", "type": "string", "default": ""},
    {"name": "dst_path", "type": "string", "default": ""},
    {"name": "image_id", "type": "string", "default": ""},
    {"name": "image_variant", "type": "string", "default": ""}
  ]
}`

// ImageUploadedEvent is the Avro schema for Image uploaded messages.
var ImageUploadedEvent = &avro.Schema{
	Definition: imageUploadedEvent,
}

// ImagePublishedEvent is the Avro schema for Image published messages.
var ImagePublishedEvent = &avro.Schema{
	Definition: imagePublishedEvent,
}
