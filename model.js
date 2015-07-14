(function () {
  'use strict';

  /////////////////////////
  // Third-party modules //
  /////////////////////////

  var mysql = require('mysql');
  var q = require('q');
  q.longStackSupport = true;
  var format = require('util').format;

  /////////////////////////
  // Config declarations //
  /////////////////////////

  /**
   * Conexões para os bancos, vide configuração anterior
   * @type {Array}
   */
  var conex = [
    mysql.createConnection({
      socketPath: '/var/run/mysqld/mysqld.sock',
      user: 'root',
      password: 'amigen',
      database: 'banco'
    }),
    mysql.createConnection({
       host: '127.0.0.1',
       port: 49153,
       user: 'root',
       password: 'amigen',
       database: 'banco'
    })
  ];

  /**
   * Indica se há uma transação em andamento ou não. Se sim, contêm o id da
   * transação, se não é nulo
   * @type {String|null}
   */
  var transactionId = null;

  //////////////////////////
  // Exporting the module //
  /////////////////////////

  /**
   * Exporta o módulo
   * @type {Object}
   */
  module.exports = {
    transfer: transfer
  };

  //////////////////////////
  // Classes declarations //
  //////////////////////////

  /**
   * Declaração do global id da conta
   * @param {String} id String com o número global
   */
  function GlobalId (id) {
    if(!/\d+\-\d+/.test(id)) {
      throw new Error('Invalid global id');
    }

    id = id.split('-');

    this.agencia = parseInt(id[0]);
    this.conta = parseInt(id[1]);

    if(conex[this.agencia] === undefined) {
      throw new TransactionError('Invalid agency identifier');
    }
  }

  GlobalId.prototype.toString = function globalIdToString() {
    return format('%d-%d', this.agencia, this.conta);
  }

  function TransactionError (msg) {
    this.name = 'TransactionError';
    this.message = msg;
    this.stack = Error.prototype.stack;
  }

  TransactionError.prototype = Error.prototype;

  //////////////////////////////
  // Function implementations //
  //////////////////////////////

  /**
   * Makes the transfer to the originating account to the dest
   * @param  {String} orig String representing the original account
   * @param  {String} dest String representing the destintation account
   * @param  {String} val  String representing the value of the transfer
   * @return {Promise}     Returns a promise of the result
   */
  function transfer (orig, dest, val) {
    orig = new GlobalId(orig);
    dest = new GlobalId(dest);

    return startTransaction()
      .then(function () {
        if(orig.agencia === dest.agencia) {
          return startTransactionDb(orig, transactionId);
        } else {
          return startTransactionDb(orig, transactionId)
            .then(function () {
              return startTransactionDb(dest, transactionId);
            });
        }
      })
      .then(function () {
        return checkAccountExists(orig);
      })
      .then(function () {
        return checkAccountExists(dest);
      })
      .then(function () {
        return checkSaldo(orig, val);
      })
      .then(function () {
        return removesFrom(orig, val);
      })
      .then(function () {
        return addsTo(dest, val);
      })
      .then(function () {
        if(orig.agencia === dest.agencia) {
          return endTransaction(orig, transactionId);
        } else {
          return endTransaction(orig, transactionId)
            .then(function () {
              return endTransaction(dest, transactionId);
            });
        }
      })
      .then(function () {
        if(orig.agencia === dest.agencia) {
          return prepareTransaction(orig, transactionId);
        } else {
          return prepareTransaction(orig, transactionId)
            .then(function () {
              return prepareTransaction(dest, transactionId);
            });
        }
      })
      .then(function () {
        if(orig.agencia === dest.agencia) {
          return commitTransaction(orig, transactionId);
        } else {
          return commitTransaction(orig, transactionId)
            .then(function () {
              return commitTransaction(dest, transactionId);
            });
        }
      })
      .then(function () {
        return closeConnections(orig, dest);
      })
      .catch(function (err) {
        return rollbackTransactions(orig, dest, err);
      });
  }

  /**
   *
   * @param  {[type]} orig [description]
   * @param  {[type]} dest [description]
   * @return {[type]}      [description]
   */
  function startTransaction (orig, dest) {
    return q.Promise(function (resolve, reject) {
      if(transactionId !== null) {
        return reject(new TransactionError('A transaction is in already effect'));
      }

      transactionId = String(new Date().getTime());

      console.log('Starting transaction with id:', transactionId);

      resolve();
    });
  }

  function runQuery (conn, queryStr, params) {
    var def = q.defer();

    conn.query(queryStr, params, function (err, res) {
      if(err) {
        def.reject(err);
      } else {
        def.resolve(res);
      }
    });

    return def.promise;
  }

  /**
   * [startTransactionDb description]
   * @param  {[type]} conn [description]
   * @param  {[type]} id   [description]
   * @return {[type]}      [description]
   */
  function startTransactionDb (acc, id) {
    var conn = conex[acc.agencia];
    var query = 'XA START ?';
    var data = [id];
    return runQuery(conn, query, data);
  }

  function checkAccountExists (id) {
    var conn = conex[id.agencia];
    var query = 'SELECT (COUNT(*) = 1) as hasConta FROM contas WHERE id = ?';
    return runQuery(conn, query, [id.conta])
      .then(function (result) {
        var exists = Boolean(Number(result[0].hasConta));
        if(!exists) {
          throw new TransactionError('Account ' + id.toString() + ' does not exist');
        } else {
          return exists;
        }
      });
  }

  function checkSaldo (acc, val) {
    var db = conex[acc.agencia];
    var query = 'SELECT (saldo > ?) as hasSaldo FROM contas WHERE id = ?';
    var data = [val, acc.conta];
    return runQuery(db, query, data)
      .then(function (result) {
        var hasSaldo = Boolean(Number(result[0].hasSaldo));
        if(!hasSaldo) {
          throw new TransactionError('A conta ' + acc.toString() + ' não tem saldo para bancar ' + val + ' para a transferência');
        } else {
          return hasSaldo;
        }
      });
  }

  function removesFrom (acc, val) {
    var db = conex[acc.agencia];
    var query = 'UPDATE contas SET saldo = (saldo - ?) WHERE id = ?';
    var data = [val, acc.conta];''
    return runQuery(db, query, data);
  }

  function addsTo (acc, val) {
    var db = conex[acc.agencia];
    var query = 'UPDATE contas SET saldo = (saldo + ?) WHERE id = ?';
    var data = [val, acc.conta];
    return runQuery(db, query, data);
  }

  function endTransaction (acc) {
    console.log('Ending transaction', transactionId);
    var db = conex[acc.agencia];
    var data = [transactionId];
    return runQuery(db, 'XA END ?', data);
  }

  function prepareTransaction (acc) {
    console.log('Preparing transaction', transactionId);
    var db = conex[acc.agencia];
    var data = [transactionId];
    return runQuery(db, 'XA PREPARE ?', data);
  }

  function commitTransaction (acc) {
    console.log('Commiting transaction', transactionId);
    var db = conex[acc.agencia];
    var data = [transactionId];
    return runQuery(db, 'XA COMMIT ?', data);
  }

  function closeConnections (orig, dest) {
    return q.Promise(function (resolve, reject) {
      try {
        transactionId = null;
      } catch(e) {
        return reject(e);
      }

      resolve({ success: true });
    });
  }

  function rollbackTransactions (orig, dest, err) {
    console.log(err);
    console.log('Rolling back transaction');
    var data = [transactionId];
    var query = 'XA ROLLBACK ?';

    var promise = endTransaction(orig)
      .then(function () {
        return runQuery(conex[orig.agencia], query, data);
      });

    if(orig.agencia === dest.agencia) {
      transactionId = null;
      return promise
        .then(function () {
          throw err;
        });
    } else {
      return promise
        .then(function () {
          return endTransaction(dest);
        })
        .then(function () {
          return runQuery(conex[dest.agencia], query, data)
        }).then(function () {
          transactionId = null;
          throw err;
        });
    }
  }

})();