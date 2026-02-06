// Sample MongoDB data initialization
// Run with: mongo < init.js

db = db.getSiblingDB('testdb');
db.testcoll.insertMany([
  { _id: '1', name: 'Alice', value: 10.5 },
  { _id: '2', name: 'Bob', value: 20.0 }
]);
