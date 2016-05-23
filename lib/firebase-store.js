'use strict';
var Assert = require('assert');
var Firebase = require('firebase');
var R = require('ramda');

var name = 'firebase-store';

module.exports = function(options) {
  var seneca = this;
  var spec;
  var firebase;
  var ref;
  var authData;

  function configure (specification, cb) {
    Assert(specification);
    Assert(cb);

    spec = specification;
    Assert(spec.token);

    var config = spec.config;
    Assert(config)
    Assert(config.databaseURL);
    Assert(config.serviceAccount);

    console.log(config);
    firebase = Firebase.initializeApp(config);

    ref = firebase.database().ref();

    if (spec.namespace) {
      ref = ref.child(spec.namespace);
    }

    cb(null, store);
  }

  var store = {
    name: name,

    close: function(cmd, cb) {
      firebase = null;
      ref = null;
    },

    save: function(args, cb) {
      Assert(args);
      Assert(cb);
      Assert(args.ent);

      var ent = args.ent;
      var table = tablename(ent);

      if (ent.id) {
        ref.child(table).child(ent.id).set(makeentp(ent), function(err) {
          if (err) { return cb(err); }
          cb(null, ent);
        });
      } else {
        var childRef = ref.child(table).push(makeentp(ent), function(err) {
          if (err) { return cb(err); }
        });

        childRef.on('value', function(snapshot) {
          ent.id = snapshot.key
          childRef.off();
          cb(null, ent);
        });
      }
    },

    load: function(args, cb) {
      Assert(args);
      Assert(cb);
      Assert(args.qent);
      Assert(args.q);
      Assert(args.q.id);

      var qent = args.qent;
      var table = tablename(qent);
      var tableRef = ref.child(table)
      var childRef = tableRef.child(args.q.id)

      function onValue(snapshot) {
        childRef.off('value');
        cb(null, makeent(qent, snapshot));
      }

      childRef.on('value', onValue);
    },

    list: function(args, cb) {
      Assert(args);
      Assert(cb);
      Assert(args.qent);
      Assert(args.q);

      var q = args.q;
      var qent = args.qent;
      var table = tablename(qent);
      var tableRef = ref.child(table);
      var results = [];

      if (q.length === 0) {
        tableRef.on('child_added', function(snapshot) {
          results.push(makeent(snapshot));
        });

        tableRef.on('value', function() {
          tableRef.off();
          cb(null, results);
        });
      } else {
        var primary = R.head(q);
        var secondary = R.tail(q);

        var query = tableRef.orderByChild(primary.key).equalTo(primary.value);
        var secondaryObj = R.reduce(function(m, n) {
          return R.assoc(n.key, n.value, m);
        }, {});
        var match = R.whereEq(secondaryObj);

        query.on('child_added', function(snapshot) {
          if (match(snapshot.val())) {
            results.push(makeent(snapshot));
          }
        });

        query.on('value', function() {
          query.off();
          cb(null, results);
        });
      }
    },

    remove: function(args, cb) {
      Assert(args);
      Assert(cb);
      Assert(args.qent);
      Assert(args.q);

      var qent = args.qent;
      var q = args.q;
      var table = tablename(qent);
      var childRef = ref.child(table).child(qent.id);

      console.log('cr remove()')
      childRef.remove()
        .then(function() { console.log('removed'); cb(null); })
        .catch(function(err) { console.log('err removing'); cb(err); });
    },

    native: function(args, cb) {
      cb(null, ref);
    }
  };

  var meta = seneca.store.init(seneca, options, store);
  var desc = meta.desc;

  console.log(R.keys(store));

  seneca.add({init: store.name, tag: meta.tag}, function(args, cb) {
    configure(options, function(err) {
      if (err) {
        return seneca.fail({code: 'entity/configure', store: store.name, error: err, desc: desc}, cb);
      }
      cb(null);
    });
  });

  return { name: store.name, tag: meta.tag };
}

function tablename (entity) {
  var canon = entity.canon$({object: true})
  return (canon.base ? canon.base + '_' : '') + canon.name
}

function makeent (ent, ref) {
  return ent.make$(R.assoc('id', ref.key, ref.val()));
}

function makeentp (ent) {
  var fields = ent.fields$();
  var entp = {};

  for(var i = 0; i < fields.length; i++) {
    entp[fields[i]] = ent[fields[i]];
  }

  return entp;
}
