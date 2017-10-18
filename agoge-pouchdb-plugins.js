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

exports.identity = async function(type) {
	await db.upsert(identityMapReduce);

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


exports.upsert = async function(doc) {
	module.upserted = module.upserted || [];

	var docHash = CryptoJS.MD5(JSON.stringify(doc)).toString();

	if (module.upserted.includes(docHash))
		return doc;

	module.upserted.push(docHash);

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
	var parts = doc._id.split('-');
	doc.type = parts[0];
	doc.id = Number(parts[1]) || parts[1];
	doc.modified = new Date();

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
	o._id = o._id.replace('-', '@') + '.' + docRevNo;	
	delete o._rev;
	delete o.id;
	delete o.type;
	await db.put(o);

	var ret = await this.put(doc);
	return ret;
}

exports.update = async function(fn) {
	var result = await this.allDocs({
		include_docs: true
	});

	var mods = [];

	result.rows.forEach(function(row) {
		var x = row.doc;

		var before = JSON.stringify(x);

		fn(x);

		var after = JSON.stringify(x);

		if (before != after)
			mods.push(x);
	});

	//console.log('Atualizando ' + mods.length + ' documentos');
	await this.bulkDocs(mods);
};

