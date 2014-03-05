var mysql      = require('mysql');
var async      = require('async');
var _          = require('underscore');

var connection = mysql.createConnection({
  database : '',
  host     : '',
  user     : '',
  password : '',
  insecureAuth: true
});

connection.connect();

var closeConn = function() {
  connection.end();
}

var getAllCustomersByEmail = function(email, cb) {

  var sql = 'select c.uid, c.email, \
                ( select count(*) from retailer_orders where customerID = c.uid) as orderCount \
             from customers c \
             where c.email = \'' + email + '\''

  connection.query(sql, function(err, rows, fields) {
    cb(err, rows)
  })

}

var deleteCustomer = function(id, cb) {
  var sql = 'delete from customers where uid = ' + id
  connection.query(sql, function(err, rows, fields) {
    cb(null, id + ' #recs: ' + rows.affectedRows)
  })
}

connection.query('SELECT email, count(*) from customers group by email order by count(*) desc', function(err, rows, fields) {
  if (err) throw err;

  async.forEach(rows, function(row, callback) {

    getAllCustomersByEmail(row.email, function(err, customers) {
      if (err) {
        console.log('Error', err)
        return
      }

      var toDelete = _.filter(customers, function(customer) { return customer.orderCount == 0 })
      var toKeep = _.filter(customers, function(customer) { return customer.orderCount > 0 })

      var toDeleteIds = _.pluck(toDelete, 'uid')
      console.log('Number To Delete:', toDelete.length, 'Number To Keep:', toKeep.length)

      if (toDeleteIds.length > 0) {
        var sql = 'delete from customers where uid in ( ' + toDeleteIds.join(', ') + ')'
        connection.query(sql, function(err, data, fields) {
          console.log('SQL Removed: ' + data.affectedRows)
          callback()
        })     
      } else {
        console.log('Nothing to delete for this email.')
        callback()
      }
    })

    
  }, function(err) {
     console.log('All Done')
  })


});

