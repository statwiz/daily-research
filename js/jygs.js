function wordsToBytes(e) {
    for (var t = [], b = 0; b < 32 * e.length; b += 8)
        t.push(e[b >>> 5] >>> 24 - b % 32 & 255);
    return t
}
function bytesToWords(e) {
    for (var t = [], i = 0, b = 0; i < e.length; i++,
    b += 8)
        t[b >>> 5] |= e[i] << 24 - b % 32;
    return t
}

function l(t) {
return null != t && (n(t) || function(t) {
    return "function" == typeof t.readFloatLE && "function" == typeof t.slice && n(t.slice(0, 0))
}(t) || !!t._isBuffer)
}
r={
endian: function(e) {
    if (e.constructor == Number)
        return 16711935 & r.rotl(e, 8) | 4278255360 & r.rotl(e, 24);
    for (var i = 0; i < e.length; i++)
        e[i] = r.endian(e[i]);
    return e
},
rotl: function(e, b) {
    return e << b | e >>> 32 - b
},
bytesToHex: function(e) {
    for (var t = [], i = 0; i < e.length; i++)
        t.push((e[i] >>> 4).toString(16)),
        t.push((15 & e[i]).toString(16));
    return t.join("")
}
};

(c = function(e, t) {
var n = {
utf8: {
    stringToBytes: function(e) {
        return n.bin.stringToBytes(unescape(encodeURIComponent(e)))
    },
    bytesToString: function(e) {
        return decodeURIComponent(escape(n.bin.bytesToString(e)))
    }
},
bin: {
    stringToBytes: function(e) {
        for (var t = [], i = 0; i < e.length; i++)
            t.push(255 & e.charCodeAt(i));
        return t
    },
    bytesToString: function(e) {
        for (var t = [], i = 0; i < e.length; i++)
            t.push(String.fromCharCode(e[i]));
        return t.join("")
    }
}
};
function l(t) {
return null != t && (n(t) || function(t) {
    return "function" == typeof t.readFloatLE && "function" == typeof t.slice && n(t.slice(0, 0))
}(t) || !!t._isBuffer)
}
n.utf8.stringToBytes(e) ? e = t && "binary" === t.encoding ? d.stringToBytes(e) : n.utf8.stringToBytes(e) : l(e) ? e = Array.prototype.slice.call(e, 0) : Array.isArray(e) || Array === Uint8Array || (e = e.toString());

for (var n = bytesToWords(e), h = 8 * e.length, a = 1732584193, b = -271733879, f = -1732584194, m = 271733878, i = 0; i < n.length; i++)
    n[i] = 16711935 & (n[i] << 8 | n[i] >>> 24) | 4278255360 & (n[i] << 24 | n[i] >>> 8);
n[h >>> 5] |= 128 << h % 32,
n[14 + (h + 64 >>> 9 << 4)] = h;
var _ = c._ff
  , v = c._gg
  , y = c._hh
  , w = c._ii;
for (i = 0; i < n.length; i += 16) {
    var k = a
      , M = b
      , x = f
      , dd = m;
    a = _(a, b, f, m, n[i + 0], 7, -680876936),
    m = _(m, a, b, f, n[i + 1], 12, -389564586),
    f = _(f, m, a, b, n[i + 2], 17, 606105819),
    b = _(b, f, m, a, n[i + 3], 22, -1044525330),
    a = _(a, b, f, m, n[i + 4], 7, -176418897),
    m = _(m, a, b, f, n[i + 5], 12, 1200080426),
    f = _(f, m, a, b, n[i + 6], 17, -1473231341),
    b = _(b, f, m, a, n[i + 7], 22, -45705983),
    a = _(a, b, f, m, n[i + 8], 7, 1770035416),
    m = _(m, a, b, f, n[i + 9], 12, -1958414417),
    f = _(f, m, a, b, n[i + 10], 17, -42063),
    b = _(b, f, m, a, n[i + 11], 22, -1990404162),
    a = _(a, b, f, m, n[i + 12], 7, 1804603682),
    m = _(m, a, b, f, n[i + 13], 12, -40341101),
    f = _(f, m, a, b, n[i + 14], 17, -1502002290),
    a = v(a, b = _(b, f, m, a, n[i + 15], 22, 1236535329), f, m, n[i + 1], 5, -165796510),
    m = v(m, a, b, f, n[i + 6], 9, -1069501632),
    f = v(f, m, a, b, n[i + 11], 14, 643717713),
    b = v(b, f, m, a, n[i + 0], 20, -373897302),
    a = v(a, b, f, m, n[i + 5], 5, -701558691),
    m = v(m, a, b, f, n[i + 10], 9, 38016083),
    f = v(f, m, a, b, n[i + 15], 14, -660478335),
    b = v(b, f, m, a, n[i + 4], 20, -405537848),
    a = v(a, b, f, m, n[i + 9], 5, 568446438),
    m = v(m, a, b, f, n[i + 14], 9, -1019803690),
    f = v(f, m, a, b, n[i + 3], 14, -187363961),
    b = v(b, f, m, a, n[i + 8], 20, 1163531501),
    a = v(a, b, f, m, n[i + 13], 5, -1444681467),
    m = v(m, a, b, f, n[i + 2], 9, -51403784),
    f = v(f, m, a, b, n[i + 7], 14, 1735328473),
    a = y(a, b = v(b, f, m, a, n[i + 12], 20, -1926607734), f, m, n[i + 5], 4, -378558),
    m = y(m, a, b, f, n[i + 8], 11, -2022574463),
    f = y(f, m, a, b, n[i + 11], 16, 1839030562),
    b = y(b, f, m, a, n[i + 14], 23, -35309556),
    a = y(a, b, f, m, n[i + 1], 4, -1530992060),
    m = y(m, a, b, f, n[i + 4], 11, 1272893353),
    f = y(f, m, a, b, n[i + 7], 16, -155497632),
    b = y(b, f, m, a, n[i + 10], 23, -1094730640),
    a = y(a, b, f, m, n[i + 13], 4, 681279174),
    m = y(m, a, b, f, n[i + 0], 11, -358537222),
    f = y(f, m, a, b, n[i + 3], 16, -722521979),
    b = y(b, f, m, a, n[i + 6], 23, 76029189),
    a = y(a, b, f, m, n[i + 9], 4, -640364487),
    m = y(m, a, b, f, n[i + 12], 11, -421815835),
    f = y(f, m, a, b, n[i + 15], 16, 530742520),
    a = w(a, b = y(b, f, m, a, n[i + 2], 23, -995338651), f, m, n[i + 0], 6, -198630844),
    m = w(m, a, b, f, n[i + 7], 10, 1126891415),
    f = w(f, m, a, b, n[i + 14], 15, -1416354905),
    b = w(b, f, m, a, n[i + 5], 21, -57434055),
    a = w(a, b, f, m, n[i + 12], 6, 1700485571),
    m = w(m, a, b, f, n[i + 3], 10, -1894986606),
    f = w(f, m, a, b, n[i + 10], 15, -1051523),
    b = w(b, f, m, a, n[i + 1], 21, -2054922799),
    a = w(a, b, f, m, n[i + 8], 6, 1873313359),
    m = w(m, a, b, f, n[i + 15], 10, -30611744),
    f = w(f, m, a, b, n[i + 6], 15, -1560198380),
    b = w(b, f, m, a, n[i + 13], 21, 1309151649),
    a = w(a, b, f, m, n[i + 4], 6, -145523070),
    m = w(m, a, b, f, n[i + 11], 10, -1120210379),
    f = w(f, m, a, b, n[i + 2], 15, 718787259),
    b = w(b, f, m, a, n[i + 9], 21, -343485551),
    a = a + k >>> 0,
    b = b + M >>> 0,
    f = f + x >>> 0,
    m = m + dd >>> 0
}
return r.endian([a, b, f, m])
})._ff = function(a, b, e, t, n, s, r) {
var o = a + (b & e | ~b & t) + (n >>> 0) + r;
return (o << s | o >>> 32 - s) + b
}
,
c._gg = function(a, b, e, t, n, s, r) {
var o = a + (b & t | e & ~t) + (n >>> 0) + r;
return (o << s | o >>> 32 - s) + b
}
,
c._hh = function(a, b, e, t, n, s, r) {
var o = a + (b ^ e ^ t) + (n >>> 0) + r;
return (o << s | o >>> 32 - s) + b
}
,
c._ii = function(a, b, e, t, n, s, r) {
var o = a + (e ^ (b | ~t)) + (n >>> 0) + r;
return (o << s | o >>> 32 - s) + b
}
function e(e, t) {
if (null == e)
    throw new Error("Illegal argument " + e);
var n = wordsToBytes(c(e, t));
return t && t.asBytes ? n : t && t.asString ? d.bytesToString(n) : r.bytesToHex(n)
}
function y() {
    return e
}
function jiemi(timestamp){
str_ing="Uu0KfOB8iUP69d3c:"+timestamp

res=y()(str_ing)
console.log(res)
return res}