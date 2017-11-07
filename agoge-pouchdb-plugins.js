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

				for ( var key in doc ) {
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
				for ( var key in doc ) {
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

var ftsView = {
	_id: "_design/fts" ,
	views: {
		fts: {
			map: function(doc) {
				try {
					var p = doc._id.indexOf('-');
					if ( p < 0 ) return;
					var type = doc._id.slice(0, p);
					var id = doc._id.slice(p+1, doc._id.length);

					if ( type == 'meta' ) return;

					var fx = function(o, prev) {
						prev = prev || '';

						for ( var key in o ) {
							if ( key[0] == '_' ) continue;
							var classe = Object.prototype.toString.call(o[key]);

							if ( classe == '[object Object]' || classe == '[object Array]') {
								fx(o[key], prev + '.' + key);
							} else if ( o[key] ) {
								var str = o[key].toString();

								var words = str
									.replace(/<(.|\n)*?>/ig, "")
									.replace(/[^-a-z0-9_@#$\s]/ig, "")
									.toLowerCase()
									.split(' ')
								;

								for ( var i = 0; i < words.length; i++ ) {
									var word = words[i];

									var highlighting = [];
									highlighting.push((prev ? prev + '.' : '') + key + ': ');

									if ( words[i-3] ) highlighting.push(words[i-3]);
									if ( words[i-2] ) highlighting.push(words[i-2]);
									if ( words[i-1] ) highlighting.push(words[i-1]);

									highlighting.push('<strong>' + word + '</strong>');

									if ( words[i+1] ) highlighting.push(words[i+1]);
									if ( words[i+2] ) highlighting.push(words[i+2]);
									if ( words[i+3] ) highlighting.push(words[i+3]);

									emit(word, {
										type: doc.type
										, id: doc.id
										, desc: doc.desc
										, highlighting: highlighting.join(' ')
									});
								}
							}
						}

					}

					fx(doc);
				} catch(err) {
					emit(null, err);
				}
			}.toString()
		}
	}
};

var metaMetaDoc = {
	_id: 'meta-meta'
	, type: 'meta'
	, id: 'meta'
	, description: 'Meta'
	, lastDatabaseMaintence: new Date(0)
}

exports.maintence = async function(force = false) {
	var mm = await this.getsert(metaMetaDoc);

	if ( ! mm.lastDatabaseMaintence ) {
		mm.lastDatabaseMaintence = new Date(0);
		mm = await this.upget(mm);
	}

	var day = 1000 * 3600 * 24;
	var now = new Date();
	var last = new Date(mm.lastDatabaseMaintence)

	var interval = now.getTime() - last.getTime();

	if ( interval < day && force == false) {
		console.log('DB: ' + last);
		return;
	} else {
		mm.lastDatabaseMaintence = now;
		await this.save(mm);
	}



	console.log('D A T A B A S E  M A I N T E N C E  S T A R T');

	await this.upsert(identityMapReduce);
	await this.buildViewIndex('identity');

	await this.upsert(whereMapReduce);
	await this.buildViewIndex('where');

	await this.upsert(ftsView);
	await this.buildViewIndex('fts');


	await this.upsert(listGlobalFields);
	await this.buildViewIndex('gfields');

	await this.createMangoIndex(['type']);
	await this.createMangoIndex(['type', 'desc']);

	var gfields = await this.query('gfields', {
		reduce: true
		, group: true
	});

	for ( row of gfields.rows ) {
		await this.createMangoIndex(['type', row.key]);
	}

	console.log();
	await this.cleanDeletedTypes();

	console.log('D A T A B A S E  M A I N T E N C E  D O N E');
}


exports.buildViewIndex = async function(name) {
	while (true) {
		try {
			await this.query(name, {key: 0, limit: 1});
			console.log(name + '  index build successfully');
			break;
		} catch(err) {
			console.log('couchdb is busy right now, delaying index building of: '  + name);
			await new Promise((resolve, reject)=>{setTimeout(resolve, 1000*60*10*Math.random())});
		}
	}
}

exports.cleanDeletedTypes = async function() {
	console.log('cleanDeletedTypes started');

	await this.upsert(listGlobalTypes);

	var gtypes = await this.query('gtypes', {
		reduce: true
		, group: true
	});

	var globalTypes = gtypes.rows.map(x=>x.key);

	console.log('global-types: ' + JSON.stringify(globalTypes) + '\n');

	var metas = await this.filter('meta').all();

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

exports.buildMangoIndex = async function(fields) {
	var o = {};

	for ( var key in fields ) {
		o[key] = 0;
	}

	while (true) {
		try {
			await this.find({selector: o});
			console.log(fields.join('-')+ '  index build successfully');
			break;
		} catch(err) {
			console.log('couchdb is busy right now, delaying index building of: '  + fields.join('-') + ' - ' + err.message);
			await new Promise((resolve, reject)=>{setTimeout(resolve, 1000*60*3*Math.random())});
		}
	}
}

exports.createMangoIndex = async function(fields) {
	var data = await this.createIndex({
		index: {
		  fields: fields
			, name: fields.join('-')
			, ddoc: fields.join('-')
			, type: 'json'
		}
	});

	if (data.result != "exists") {
		console.log('bulding ' + fields.join('-') + ' index');
		await this.buildMangoIndex(fields);
	}

	console.log("MANGO '" + fields.join('-') + "' INDEX: " + JSON.stringify(data.result));
}


exports.fts = function(key) {
	new Promise(async (resolve, reject) => {
		await this.upsert(ftsView);
		await this.buildViewIndex('fts');
		resolve();
	});

	return this.where('fts', {key: key.toLowerCase(), include_docs: false});
}

exports.identity = async function(type) {
	await this.upsert(identityMapReduce);
	await this.buildViewIndex('identity');

	var ret = await this.query('identity/identity', {key: type, group: true});

	if (ret.rows.length == 0)
		return 1;

	else if (ret.rows.length == 1)
		return Number(ret.rows[0].value) + 1;

	else
		throw new Error('sigedin/lastid at expected 0 or 1 found: ' + ret.rows.length);
}

exports.where = function(ddocView, options) {
	new Promise(async (resolve, reject) => {
		await this.upsert(whereMapReduce);
		await this.buildViewIndex('where');
	});

	var options = options || {};

	if ( options.include_docs !== false ) {
		options.include_docs = true;
	}

	var plugin = this;

	return new function() {
		this.take = async function(n, m) {
			if ( n ) options.limit = n;
			if ( m ) options.start_key = m;

			var results = await plugin.query(ddocView, options);

			if ( results.rows.length == 0 && options.limit == 1) {
				return;
			} else if ( results.rows.length == 0) {
				return [];
			} else if ( options.limit == 1 ) {
				var row = results.rows[0];
				return row.doc || row.value;
			} else if ( options.include_docs ) {
				return results.rows.map(x=>x.doc);
			} else if ( Object.prototype.toString.call(results.rows[0].value) == '[object Object]' ) {
				return results.rows.map(x=>x.value);
			} else {
				return results.rows;
			}
		}

		this.limit = this.take;

		this.all = this.take;

		this.page = function(pageNo, pageSize) {
			pageNo = pageNo || 1;
			pageNo = Number(pageNo);

			pageSize = pageSize || 100;
			pageSize = Number(pageSize);

			options.skip = pageNo > 1 ? pageSize * ( pageNo - 1 ) : 0;
			options.limit = pageSize;
			return this.take();
		}

		this.scalar = async function() {
			options.limit = 1;
			options.include_docs = true;
			return this.take();
		}
	}
}

exports.filter = function(/*type, [key], [value], [options]*/) {
	var type, key, value, options;

	for ( let i = 0; i < arguments.length; i++ ) {
		if ( Object.prototype.toString.call(arguments[i]) == '[object Object]' ) {
			options = arguments[i];
		} else {
			if ( i == 0 ) type = arguments[i];
						else
			if ( i == 1 ) key = arguments[i];
						else
			if ( i == 2 ) value = arguments[i];
		}
	}

	var options = options || {};
	options.key = [];

	if ( options["include_docs"] !== false )
		options["include_docs"] = true;

	if (type) options.key.push(type);
	if (key) options.key.push(key);
	if (value) options.key.push(value);

	return this.where('where', options);
}


exports.upget = async function(doc) {
	try { existing = await this.get(doc._id); } catch(err) { var existing; }
	var existingHash = CryptoJS.MD5(JSON.stringify(existing)).toString();

	var docHash = CryptoJS.MD5(JSON.stringify(doc)).toString();

	if ( docHash == existingHash ) {
		return existing;
	} else if ( existing ) {
		doc._rev = existing._rev;

		await this.save(doc);
		return await this.get(doc._id);
	} else {
		await this.save(doc);
		return await this.get(doc._id);
	}
}

exports.upsert = async function(doc, byPassSingleton = false) {
	if ( ! byPassSingleton )  {
		module.upserted = module.upserted || [];
		var docHash = CryptoJS.MD5(JSON.stringify(doc)).toString();
		if (module.upserted.includes(docHash)) return doc;
		module.upserted.push(docHash);
	}

	try { var existing = await this.get(doc._id); } catch(err) { var existing; }

	if ( existing ) {
		var rev = existing._rev;
		delete doc._rev;
		delete existing._rev;

		var docHash = CryptoJS.MD5(JSON.stringify(doc)).toString();
		var existingHash = CryptoJS.MD5(JSON.stringify(existing)).toString();

		if ( docHash != existingHash ) {
			doc._rev = rev;			
			await this.put(doc);
		}

		return await this.get(doc._id);
	} else {
		delete doc._rev;
		await this.put(doc);
		return await this.get(doc._id);
	}
}

exports.getsert = async function(doc) {
	try {
		return await this.get(doc._id);
	} catch(err) {
		delete doc._rev;

		await this.save(doc);
		return await this.get(doc._id);
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

	if ( doc.type != 'meta' ) {
		try {
			var meta = await this.get('meta-' + doc.type);

			var descKey;

			for ( var key in meta ) {
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
	delete o._deleted;
	o.type = null;
	o.id = null;

	var ret = await this.upsert(o, true);
	return ret;
}

