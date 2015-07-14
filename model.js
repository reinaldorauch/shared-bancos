(function () {
  'use strict';

  /////////////////////////
  // Third-party modules //
  /////////////////////////

  var Client = require('mariasql');
  var q = require('q');

  /////////////////////////
  // Config declarations //
  /////////////////////////

  /**
   * Configurações dos bancos de dados
   * [0] para o banco da Agência 0
   * [1] para o banco da Agência 1
   * @type {Array}
   */
  var dbs = [
    {
      unixSocket: '/var/run/mysqld/mysqld.sock',
      user: 'root',
      password: 'amigen',
      db: 'banco'
    },
    {
       host: '127.0.0.1',
       port: '49153',
       user: 'root',
       password: 'amigen',
       db: 'banco'
    }
  ];

  /**
   * Conexões para os bancos, vide configuração anterior
   * @type {Array}
   */
  var conex = [
    new Client(),
    new Client()
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
    console.log(id);

    this.agencia = parseInt(id[0]);
    this.conta = parseInt(id[1]);

    if(conex[this.agencia] === undefined || dbs[this.agencia] === undefined) {
      throw new Error('Invalid agency identifier');
    }
  }

  GlobalId.prototype.toString = function globalIdToString() {
    return format('%d-%d', this.agencia, this.conta);
  }

  function TransactionError () {
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
      .then(q.all([connect(orig), connect(dest)]))
      .then(q.all([startTransactionDb(orig, transactionId), startTransactionDb(dest, transactionId)]))
      .then(q.all([checkAccountExists(orig), checkAccountExists(dest)]))
      .then(checkSaldo(orig, val))
      .then(doTransfer(orig, dest, val))
      .then(q.all([endTransaction(orig), endTransaction(dest)]))
      .then(q.all([prepareTransaction(orig), prepareTransaction(dest)]))
      .then(q.all([commitTransaction(orig), commitTransaction(dest)]))
      .then(closeConnections(orig, dest))
      ['catch'](rollbackTransactions.bind(null, orig, dest));
  }

  /**
   * Connects to the databases to perform the transfer
   * @param  {GlobalId} acc Identificador da agÊncia
   * @return {Promise}          [description]
   */
  function connect (acc) {
    var def = q.defer();
    var client = conex[acc.agencia];
    var db = dbs[acc.agencia];

    client.on('connect', function  () {
      console.log('Connected to the database', db);
      def.resolve(client);
    });

    client.on('error', function (err) {
      console.log('Error on connecting to the database:', err.message);
      console.log(err.trace);
      def.reject(err);
    });

    client.on('close', function () {
      console.log('Client closed');
    });

    client.connect(db);

    return def.promise;
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

  function runQuery (conn, query, params) {
    var def = q.defer();
    query = conn.query(query, params);

    query.on('result', function (res) {
      var result = [];
      res.on('row', function  (row) {
        console.log(row);
        def.notify(row);
        result.push(row);
      });

      res.on('error', function (err) {
        def.reject(err);
      });

      res.on('end', function () {
        def.resolve(result);
      });
    });

    query.on('error', function (err) {
      def.reject(err);
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
    var query = 'XA START :id';
    var data = {id: id};
    return runQuery(conn, query, data);
  }

  function checkAccountExists (id) {
    return runQuery(conex[id.agencia], 'SELECT (COUNT(*) = 1) as exists FROM contas WHERE id = :id', {id: id.conta})
      .then(function (result) {
        var exists = Boolean(Number(result[0].exists));
        if(!exists) {
          throw new TransactionError('Account ' + id.toString() + ' does not exist');
        } else {
          return exists;
        }
      });
  }

  function checkSaldo (acc, val) {
    var db = conex[acc.agencia];
    var query = 'SELECT (saldo > :val) as hasSaldo FROM contas WHERE id = :id';
    var data = { val: val, id: acc.conta };
    return runQuery(db, query, data)
      .then(function (res) {
        var hasSaldo = Boolean(Number(result[0].hasSaldo));
        if(!hasSaldo) {
          throw new TransactionError('A conta ' + id.toString() + ' não tem saldo para bancar ' + val + ' para a transferência');
        } else {
          return hasSaldo;
        }
      });
  }

  function doTransfer (orig, dest, val) {
    return q.all([removesFrom(orig, val), addsTo(dest, val)]);
  }

  function removesFrom (acc, val) {
    var db = conex[acc.agencia];
    var query = 'UPDATE contas SET saldo = (saldo - :val) WHERE id = :id';
    var data = { val: val, id: acc.conta };
    return runQuery(db, query, data);
  }

  function addsTo (acc, val) {
    var db = conex[acc.agencia];
    var query = 'UPDATE contas SET saldo = (saldo + :val) WHERE id = :id';
    var data = { val: val, id: acc.conta };
    return runQuery(db, query, data);
  }

  function endTransaction (acc) {
    console.log('Ending transaction', transactionId);
    var db = conex[acc.agencia];
    var data = { id: transactionId };
    return runQuery(db, 'XA END :id', data);
  }

  function prepareTransaction (acc) {
    console.log('Preparing transaction', transactionId);
    var db = conex[acc.agencia];
    var data = { id: transactionId };
    return runQuery(db, 'XA PREPARE :id', data);
  }

  function commitTransaction (acc) {
    console.log('Commiting transaction', transactionId);
    var db = conex[acc.agencia];
    var data = { id: transactionId };
    return runQuery(db, 'XA COMMIT :id', data);
  }

  function closeConnections (orig, dest) {
    return q.Promise(function (resolve, reject) {
      try {
        transactionId = null;
        conex[orig.agencia].end();
        conex[dest.agencia].end();
      } catch(e) {
        return reject(e);
      }

      resolve({ success: true });
    });
  }

  function rollbackTransactions (orig, dest, err) {
    console.log(err.trace);
    console.log('Rolling back transaction');
    transactionId = null;
    var data = { id: transactionId };
    var query ='XA ROLLBACK :id';
    return q.all([
      runQuery(conex[orig.agencia], query, data),
      runQuery(conex[dest.agencia], query, data)
    ]).then(function () {
      throw err;
    });
  }

})();