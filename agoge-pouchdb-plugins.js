var CryptoJS = require('crypto-js');

var identityMapReduce = {
	_id: "_design/identity",
	views: {
		identity: {
			map: function (doc) {
				var p = doc._id.indexOf('-');
				if ( p < 0 ) return;
				var type = doc._id.slice(0, p);
				var id = Number(doc._id.slice(p+1, doc._id.length));

				emit(type, id);
			}.toString(),
			reduce: function (keys, values, rereduce) {
				return values.sort(function(a,b) {return a-b}).reverse()[0];
			}.toString()
		}
	}
}

var whereMapReduce = {
	_id: "_design/where" ,
	views: {
		where: {
			map: function(doc) {
				var p = doc._id.indexOf('-');
				if ( p < 0 ) return;
				var type = doc._id.slice(0, p);
				var id = doc._id.slice(p+1, doc._id.length);

				emit([type]);

				if ( isNaN(id) ) {
					emit([type, id]);
				} else {
					emit([type, Number(id)]);
				}

				for (key in doc) {
				  emit([type, key, doc[key]]);
				}
			}.toString()
		}
	}
};

var listGlobalFields = {
	_id: "_design/gfields" ,
	views: {
		gfields: {
			map: function(doc) {
				for (key in doc) {
					if ( key != 'type' && key.slice(0,1) != '_' ) {
						emit(key);
					}
				}
			}.toString()
			, reduce: "_count"
		}
	}
};

var listGlobalTypes = {
	_id: "_design/gtypes" ,
	views: {
		gtypes: {
			map: function(doc) {
				if (doc.type) emit(doc.type);
			}.toString()
			, reduce: "_count"
		}
	}
};


exports.cleanDeletedTypes = async function() {
	console.log('cleanDeletedTypes started');

	await this.upsert(listGlobalTypes);

	var gtypes = await this.query('gtypes', {
		reduce: true
		, group: true
	});
	var globalTypes = gtypes.rows.map(x=>x.key);

	console.log('global-types: ' + JSON.stringify(globalTypes) + '\n');

	var metas = await db.filter('meta').all();
	var metaTypes = metas.map(x=>x.id);

	console.log('meta-types: ' + JSON.stringify(metaTypes) + '\n');

	for ( globalType of globalTypes ) {

		if ( ! metaTypes.includes(globalType) && globalType != 'meta' ) {

			var lista = await this.filter(globalType).all();

			for ( item of lista ) {
				item._deleted = true;
			}

			console.log('deleting ' + globalType + ' with ' + lista.length + ' docs');

			while ( lista.length > 0 ) {
				let agora = [];

				for ( let i = 0; i < (lista.length > 1000 ? 1000 : lista.length); i++) {
					agora.push(lista.shift());
				}

				await this.bulkDocs(agora);
			}

			console.log(globalType + ' done' + '\n');
		}

	}

	console.log('cleanDeletedTypes done' + '\n');

}

exports.buildMangoIndex = async function(name) {
	var o = {type: 'meta' };
	if ( name ) o[name] = 0;

	while (true) {
		try {
			await this.find({selector: o});
			console.log(name + '  index build successfully');
			break;
		} catch(err) {
			console.log('couchdb is busy right now, delaying index building of: '  + name);
			await new Promise((resolve, reject)=>{setTimeout(resolve, 1000*60*10*Math.random())});
		}
	}
}

exports.createMangoTypeIndex = async function() {
	var data = await db.createIndex({
		index: {
		  fields: ['type']
		}
		, name: 'type'
		, ddoc: 'type'
		, type: 'json'
	});

	this.buildMangoIndex();
	console.log("MANGO 'type' INDEX: " + JSON.stringify(data.result));
}

exports.createMangoIndex = async function(name) {
	var data = await db.createIndex({
		index: {
		  fields: ['type', name]
			, name: 'type-' + name
			, ddoc: 'type-' + name
			, type: 'json'
		}
	});

	this.buildMangoIndex(name);

	console.log("MANGO '" + name + "' INDEX: " + JSON.stringify(data.result));
}

exports.init = async function() {
	await this.upsert(listGlobalFields);

	await this.createMangoTypeIndex();
	await this.createMangoIndex('desc');

	var gfields = await this.query('gfields', {
		reduce: true
		, group: true
	});

	for ( row of gfields.rows ) {
		await this.createMangoIndex(row.key);
	}

	console.log('db.init done');
}


exports.identity = async function(type) {
	await this.upsert(identityMapReduce);

	var ret = await this.query('identity/identity', {key: type, group: true});

	if (ret.rows.length == 0)
		return 1;

	else if (ret.rows.length == 1)
		return Number(ret.rows[0].value) + 1;

	else
		throw new Error('sigedin/lastid at expected 0 or 1 found: ' + ret.rows.length);
}

exports.tolist = async function(ddoc, view, options) {
	var options = options || {};
	var view = view || ddoc;

	if ( options["include_docs"] !== false)
		options["include_docs"] = true;

	var queryResults = await this.query(ddoc + '/' + view, options);

	var list = [];

	for (resultRow of queryResults.rows) {
		list.push(resultRow.doc);
	}

	return list;
}

exports.scalar = async function(ddoc, view, options) {
	var options = options || {};

	if ( options["include_docs"] !== false)
		options["include_docs"] = true;

	options["limit"] = 1;

	var ret = await this.query(ddoc + '/' + view, options);

	if (ret.rows.length == 0)
		return;

	else

	if (ret.rows.length == 1) {
		var row = ret.rows[0];
		return row.doc || row.value;
	}
}

exports.first = async function(type, key, value, include_docs) {
	var options = {
		key: [],
		include_docs: include_docs || true
	};

	if (type) options.key.push(type);
	if (key) options.key.push(key);
	if (value) options.key.push(value);

	await this.upsert(whereMapReduce);

	return await this.scalar('where', 'where', options);
}

exports.filter = function(type, key, value, include_docs) {
	var options = {
		key: [],
		include_docs: include_docs || true
	};

	if (type) options.key.push(type);
	if (key) options.key.push(key);
	if (value) options.key.push(value);

	var plugin = this;

	return new function() {
		this.take = async function(n, m) {
			await plugin.upsert(whereMapReduce);

			if ( n ) {
				options.limit = n;
			}

			if ( m ) {
				options.start_key = m;
			}

			return await plugin.tolist('where', 'where', options);
		}

		this.all = this.take;

		this.page = async function(pageNo, pageSize) {
			pageNo = pageNo || 1;
			pageSize = pageSize || 100;

			await plugin.upsert(whereMapReduce);

			if ( pageNo > 1 ) {
				options.skip = pageSize * ( pageNo - 1 );
			}

			options.limit = pageSize;

			return await plugin.tolist('where', 'where', options);
		}
	}
}


exports.upsert = async function(doc, byPassSingleton = false) {
	module.upserted = module.upserte || [];

	if ( ! byPassSingleton )  {
		var docHash = CryptoJS.MD5(JSON.stringify(doc)).toString();

		if (module.upserted.includes(docHash))
			return doc;

		module.upserted.push(docHash);
	}

	try {
		delete doc._rev;

		var existing = await this.get(doc._id);
		var rev = existing._rev;
		delete existing._rev;

		if (JSON.stringify(doc) != JSON.stringify(existing)) {
			doc._rev = rev;

			try {
				return await this.put(doc);
			} catch(err) {
				return err;
			}
		} else {
			return doc;
		}

	} catch(err) {
		try {
			delete doc._rev;

			return await this.put(doc)
		} catch(err) {
			console.log(err);
			return err;
		}
	}
}

exports.save = async function(doc) {
	var doc = await this.standardizeDoc(doc);
	var rev = await this.storeRevision(doc);
	var ret = await this.put(doc);
	return ret;
}

exports.update = async function(fn) {
	var result = await this.allDocs({
		include_docs: true
	});

	var mods = [];

	for ( row of result.rows ) {
		var x = row.doc;

		var before = JSON.stringify(x);

		fn(x);

		var after = JSON.stringify(x);

		if (before != after) {
			await this.standardizeDoc(x);
			await this.storeRevision(x);
			mods.push(x);
		}
	}

	//console.log('Atualizando ' + mods.length + ' documentos');
	await this.bulkDocs(mods);
};

exports.standardizeDoc = async function(doc) {
	var parts = doc._id.split('-');
	doc.type = parts[0];
	doc.id = Number(parts[1]) || parts[1];
	doc.modified = new Date();

	try {
		var meta = await this.get('meta-' + doc.type);

		var descKey;

		for (key in meta) {
			if ( meta[key].type == "description" ) {
				var descKey = key;
				break;
			}
		}

		if ( descKey )
			doc.desc = doc[descKey];
		else
			doc.desc = doc.type.toUpperCase() + ': ' + doc.id;

	} catch(err) {
		doc.desc = doc.type.toLowerCase() + ': ' + doc.id;
	}

	return doc;
}

exports.storeRevision = async function(doc) {
	var currentVersionJson = JSON.stringify(doc, null, 4);

	if ( ! doc._rev ) {
		var docRevNo = 0;
		var docRevGuid = 0;
	} else {
		var docRevisionParts = doc._rev.split('-');
		var docRevNo = docRevisionParts[0];
		var docRevGuid = docRevisionParts[1];
	}

	var o = JSON.parse(JSON.stringify(doc));
	o._id = o._id.replace('-', '@') + '.' + (Number(docRevNo)+1);
	delete o._rev;
	o.type = null;
	o.id = null;

	var ret = await this.upsert(o);
	return ret;
}

