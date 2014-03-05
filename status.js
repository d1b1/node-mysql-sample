var mysql      = require('mysql');
var async      = require('async');
var _          = require('underscore');

var connection = mysql.createConnection({
  database : 'loopyarn01',
  host     : 'MySQLB5.webcontrolcenter.com',
  user     : 'loopyarnwebuser',
  password : 'loopyarn151',
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

connection.query('SELECT email, count(*) from customers group by email order by count(*) desc', function(err, rows, fields) {
  if (err) throw err;

  _.each(rows, function(unit) {
    getAllCustomersByEmail(unit.email, function(err, customers) {

      if (err) {
        console.log('Error', err)
        closeConn()
        return
      }

      var withOrders = 0
      var withoutOrders = 0;

      _.each(customers, function(customer) {
        if (customer.orderCount == 0) {
           withoutOrders++;
        } else {
           withOrders++;
        }
      })

      if (withoutOrders > 0)
        console.log(unit.email, 'Report Without:', withoutOrders, 'Report With:', withOrders)
    })

  })

  closeConn()


});

