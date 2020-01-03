const Crawler = require("crawler");
const { AsyncParser } = require('json2csv');
const { writeFile } = require("fs").promises;

const area = [];
const baseURL = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018';
const typeArray = ['province', 'city', 'county', 'town', 'village'];

const { level } = process.env;

console.log('level: ', level);

const fields = ['1级行政区编码', '1级行政区名称', '2级行政区编码', '2级行政区名称', '3级行政区编码', '3级行政区名称', '4级行政区编码', '4级行政区名称', '5级行政区编码', '城乡分类代码', '5级行政区名称'];
const opts = { fields };
const transformOpts = { highWaterMark: 8192 };
 
const asyncParser = new AsyncParser(opts, transformOpts);

const c = new Crawler({
    // 在每个请求处理完毕后将调用此回调函数
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20',
    maxConnections: 1,
    rateLimit: '50',
    callback(error, res, done) {
        if (error) {
            console.log(error);
        } else {
            const $ = res.$;
            const { uri, type } = res.options;

            console.log(`爬取完毕：${type}/${uri}`);
            if (type === 'province') {
                $(`.${type}tr a`).each((i, elm) => {
                    const el = $(elm);
                    const href = el.attr('href');
                    const data = {
                        name: el.text(),
                        code: `${href.split('.')[0]}0000000000`,
                    };
                    area.push(data);
                    c.queue({
                        uri: `${baseURL}/${href}`,
                        type: 'city',
                        ancestors: [data],
                    });
                })
            } else {
                const { ancestors } = res.options;
                $(`.${type}tr`).each((i, elm) => {
                    let els = $(elm).find('a');
                    if (!els.length) els = $(elm).find('td');
                    const href = $(els[0]).attr('href');

                    const data = {
                        name: $(els[els.length - 1]).text(),
                        code: $(els[0]).text(),
                    };
                    if (els.length === 3 || level === type || !href) {
                        const dataArr = [...ancestors, data];
                        const csvData = {};
                        for (const [i, item] of dataArr.entries()) {
                            csvData[`${i+1}级行政区编码`] = item.code;
                            csvData[`${i+1}级行政区名称`] = item.name;
                        }
                        csvData['城乡分类代码'] = els[2] ? $(els[1]).text() : '';
                        asyncParser.input.push(JSON.stringify(csvData));
                    } else {
                        const { uri } = res.options;
                        const i = typeArray.indexOf(type);
                        const tmp = uri.split('/');
                        tmp.pop();

                        c.queue({
                            uri: `${tmp.join('/')}/${href}`,
                            type: typeArray[i+1],
                            ancestors: [...ancestors, data],
                        });
                    }
                })
            }
        }
        done();
    }
});

let csv = '';
asyncParser.processor
  .on('data', chunk => (csv += chunk.toString()))
  .on('end', async () => {
    await writeFile(`./${level || 'data'}.csv`, csv);
    console.log('生成文件ok');
  })
  .on('error', err => console.error(err));

c.on('drain',function() {
    console.log('爬取完毕 drain');
    asyncParser.input.push(null);
});

c.queue({
    uri: baseURL,
    type: 'province',
});