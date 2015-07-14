(function () {
  'use strict';

  var server = require('restify').createServer({name: 'shared-bancos'});

  var model = require('./model');

  server.get('/transferencia/:idOrig/:idDest/:valor', handlerTransferencia);
  server.get('/', handlerRoot);

  server.listen('5000', handlerListen);

  /**
   * Handler da requisição de transferência
   * @param  {Request} req Requisição
   * @param  {Response} res Resposta da transação
   */
  function handlerTransferencia (req, res) {
    console.log('Request de transferência');
    console.log('Id de origem: ', req.params.idOrig);
    console.log('Id de destino: ', req.params.idDest);
    console.log('Valor: ', req.params.valor);

    model.transfer(req.params.idOrig, req.params.idDest, req.params.valor)
      .then(function (ret) {
        ret.msg = 'Sucesso.';
        res.json(200, ret);
      })['catch'](function (err) {
        console.error(err.message);
        res.json(500, err);
      });
  }

  /**
   * Handler para um request no root
   * @param  {Request} req Request no app
   * @param  {Response} res Resposta do app
   */
  function handlerRoot (req, res) {
    console.log('Request no root');
    res.json({msg: 'Hello world'});
  }

  /**
   * Handler de listening do servidor
   */
  function handlerListen () {
    console.log('Server listening at localhost:5000');
  }

})();