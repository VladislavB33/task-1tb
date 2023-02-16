const fs = require('fs');
const readline = require('readline');
const util = require('util');

async function sortFile(fileName, chunkSize) {
  const rl = readline.createInterface({
    input: fs.createReadStream(fileName, {highWaterMark: 1024*1024*450}),
  });

  let chunk = [];
  let chunkIndex = 0;

  for await (const line of rl) {
    chunk.push(line);

    if (chunk.length >= chunkSize) {
      chunk.sort();
      fs.writeFileSync(`chunk-${chunkIndex}.txt`, chunk.join('\n'));
      chunk = [];
      chunkIndex++;
    }
  }

  if (chunk.length > 0) {
    chunk.sort();
    fs.writeFileSync(`chunk-${chunkIndex}.txt`, chunk.join('\n'));
  }
}
function mergeChunks(outputFile) {
  const writeStream = fs.createWriteStream(outputFile);
  for (let i = 0; ; i++) {
    const file = `chunk-${i}.txt`;

    if (!fs.existsSync(file)) {
      break;
    }

    const data = fs.readFileSync(file, 'utf-8');
    writeStream.write(data);
  }

  writeStream.end();
}

const unlink = util.promisify(fs.unlink);

async function deleteChunks() {
  for (let i = 0; ; i++) {
    const file = `chunk-${i}.txt`;
    if (!fs.existsSync(file)) {
      break;
    }

    await unlink(file);
  }
}
sortFile('1tb.txt', 100000).then(() => {
  mergeChunks('sorted.txt');
  deleteChunks();
});