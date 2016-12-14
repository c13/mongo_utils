var duplicates = [];

db.dataset_evacuation_base_dupl.aggregate(

  // Pipeline
  [
    // Stage 1
    {
      $group: {
      "_id": { "regNum": "$regNum", "evacuationDate": "$evacuationDate", "typeId": "$typeId", "parkingAdress": "$parkingAdress"},
      "dups": { "$push": "$_id" },
      "count": { "$sum": 1 }
      }
    },

    // Stage 2
    {
      $match: {
      "count": { "$gt": 1 }
      }
    },

    // Stage 3
    {
      $limit: 1
    }
  ],

  // Options
  {
    allowDiskUse: true
  }

  // Created with 3T MongoChef, the GUI for MongoDB - http://3t.io/mongochef

).result.forEach(function(doc) {
    doc.dups.shift();      // First element skipped for deleting
    doc.dups.forEach( function(dupId){
        duplicates.push(dupId);   // Getting all duplicate ids
        }
    )
});

db.dataset_evacuation_base_dupl.remove({_id:{$in:duplicates}});
