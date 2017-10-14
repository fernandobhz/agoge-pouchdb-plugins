var CryptoJS = require('crypto-js');

exports.identity = async function(type) {	
	await db.upsert({
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
	});
	
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

exports.find = async function(type, key, value, all, include_docs) {
	var all = all || false;
	
	var options = {
		key: []
	};	
		
	options.include_docs = include_docs;

	if (type) options.key.push(type);
	if (key) options.key.push(key);
	if (value) options.key.push(value);
	
	await this.upsert({
		_id: "_design/find" ,
		views: {
			find: {
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

					var key;
					
					for (key in doc) {
					  emit([type, key, doc[key]]);
					}
				}.toString()
			}
		}
	});

	if (all)
		return await this.tolist('find', 'find', options);
	else
		return await this.scalar('find', 'find', options);
	
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

		
	try {
		var old = await db.get(doc._id);
	} catch (err) {
		var old = { _attachments: {} };	
	}
	
	doc._attachments = old._attachments;	

	var an = 'revisions/' + docRevNo + '.json';

	doc._attachments[an] = {
		"content_type": "application/json",
		data: new Buffer(currentVersionJson)
	}

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

