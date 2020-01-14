const sharp = require('sharp'),
			path = require('path'),
      storage = require('./storage');

let storage_handler = new storage.storage();

exports.handler = async function(event) {
  input_bucket = event.bucket.input
  output_bucket = event.bucket.output
  let key = event.object.key
  width = event.object.width
  height = event.object.height
  let pos = key.lastIndexOf('.');
  let upload_key = key.substr(0, pos < 0 ? key.length : pos) + '.png';

  const sharp_resizer = sharp().resize(width, height).png();
  let input_data = storage_handler.downloadStream(input_bucket, key);
  let [writeStream, promise] = storage_handler.uploadStream(output_bucket, upload_key);
  input_data.pipe(sharp_resizer).pipe(writeStream);
  await promise;
  return {bucket: output_bucket, key: upload_key}
};
