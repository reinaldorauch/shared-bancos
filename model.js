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

    return connect(orig)
      .then(connect(dest))
      .then(startTransaction(orig, dest))
      .then(checkAccountExists(orig))
      .then(checkAccountExists(dest))
      .then(checkSaldo(orig))
      .then(doTransfer(orig, dest, val))
      .then(commitTransfer(orig, dest))
      .then(closeConnections(orig, dest));
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

    client.connect(db);

    client.on('connect', function  () {
      def.resolve(client);
    });

    client.on('error', function (err) {
      def.reject(err);
    });

    client.on('close', function () {
      console.log('Client closed');
    });

    return def.promise;
  }

  /**
   *
   * @param  {[type]} orig [description]
   * @param  {[type]} dest [description]
   * @return {[type]}      [description]
   */
  function startTransaction (orig, dest) {
    var transId = String(date.getTime());

    orig = conex[orig.agencia];
    dest = conex[orig.agencia];


  }

})();