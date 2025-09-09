const { neon } = require("@neondatabase/serverless");
const dotenv = require('dotenv');
const readXlsxFile = require('read-excel-file/node');
const multer = require('multer');
dotenv.config();
const client = neon(process.env.db_url);
const express = require('express');
const app = express();
const path = require('path');
const cors = require('cors');
const fs = require('fs');
const xlsx = require('xlsx');

const allowedOrigins = [
  "http://localhost:3000", 
  "https://excel-system-frond-end.vercel.app"
];

app.use(cors({
  origin: function (origin, callback) {
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error("Not allowed by CORS"));
    }
  },
  methods: ["GET", "POST", "PUT", "DELETE"],
  credentials: true
}));

const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, 'uploads/');
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = file.originalname.split('.')[0] + '-' + Date.now();
    cb(null, uniqueSuffix + path.extname(file.originalname));
  }
});
const fileFilter = (req, file, cb) => {
  const filetypes = /xlsx|xls/;
  const extname = filetypes.test(path.extname(file.originalname).toLowerCase());
  const mimetype = file.mimetype === "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
                   file.mimetype === "application/vnd.ms-excel";

  if (extname && mimetype) {
    cb(null, true);
  } else {
    cb(new Error("Only Excel files are allowed!"));
  }
};
const upload = multer({ storage: storage, fileFilter });

async function read(tablename, filepath) {
  const rows = await readXlsxFile(filepath);
  if (!rows.length) {
    console.warn("⚠️ No rows in file:", filepath);
    return;
  }

  const headers = rows[0].map(h =>
    h.trim().replace(/\s+/g, "_").replace(/[^a-zA-Z0-9_]/g, "")
  );
  const firstDataRow = rows[1] || [];

  
  const columns = headers.map((col, index) => {
    const value = firstDataRow[index];
    console.log(typeof value)
    if(index==0){
       if (typeof value === "number") {
      if (Number.isInteger(value)) {
        if (value >= -2147483648 && value <= 2147483647) {
         
       
          return `"${col}" BIGINT UNIQUE`;
        }
      } else {
        return `"${col}" FLOAT UNIQUE`;
      }
    } else {
      return `"${col}" TEXT UNIQUE`;
    }

    }

    if (typeof value === "number") {
      if (Number.isInteger(value)) {
        if (value >= -2147483648 && value <= 2147483647) {
          return `"${col}" INT`;
        } else {
          return `"${col}" BIGINT`;
        }
      } else {
        return `"${col}" FLOAT`;
      }
    } else {
      return `"${col}" TEXT`;
    }
  });


  const schema = [`"uniqid" UUID PRIMARY KEY DEFAULT gen_random_uuid()`, ...columns].join(", ");



  try {
    await client`
      CREATE TABLE IF NOT EXISTS ${client.unsafe(tablename)} (
        ${client.unsafe(schema)}
      )
    `;
  } catch (err) {

    console.error("Error:", err.message);
    throw err;
  }
}



async function inserttable(tablename, filepath) {
  const rows = await readXlsxFile(filepath);
  if (!rows.length) {
    console.warn("⚠️ No rows found in Excel file:", filepath);
    return;
  }

  const headers = rows[0].map(h => h.trim());
  const columnList = headers.join(", ");

  console.log(`📥 Inserting into table "${tablename}" with columns:`, headers);

  for (const [index, row] of rows.slice(1).entries()) {
    const safeRow = headers.map((_, i) => {
      const value = row[i];
      if (value === undefined || value === null || value === "") return null;
      if (value instanceof Date) return value.toISOString();
      return value;
    });

    const placeholders = headers.map((_, i) => `$${i + 1}`).join(", ");

    try {
   await client.query(
  `INSERT INTO ${tablename} (${columnList})
   VALUES (${placeholders})
   ON CONFLICT (${headers[0]}) 
   DO UPDATE SET ${headers.slice(1).map((col, i) => `"${col}" = EXCLUDED."${col}"`).join(", ")}`,
  safeRow
);

    } catch (err) {
      console.error(`❌ Failed to insert row ${index + 1}:`, safeRow);
      console.error("Error:", err.message);
    }
  }

  console.log(`✅ Finished inserting ${rows.length - 1} rows into "${tablename}"`);
}







app.post('/upload', upload.single('excel'), async (req, res) => {
  const filepath = `./uploads/${req.file.filename}`;
  const tablename = req.file.originalname.split('.')[0];

  try {
    await read(tablename, filepath);
    await inserttable(tablename, filepath);
    res.send({ message: 'success' });
  } catch (err) {
    console.error("Upload Error:", err);
    res.send({ message: 'failed' });
  }
});

app.get('/gettables', async (req, res) => {
  try {
    const result = await client`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      ORDER BY table_name;
    `;
    res.send({ message: 'success', table: result });
  } catch (err) {
    res.send({ message: 'failed', table: [] });
  }
});

app.get('/tabledata/:tablename/:page', async (req, res) => {
  const { tablename, page } = req.params;
  try {
    const header1 = await client`
      SELECT * FROM ${client.unsafe(tablename)} LIMIT 1
    `;
    const idname = Object.keys(header1[0])[0];
    const ascid=Object.keys(header1[0])[1];

    const result = await client`
      SELECT * FROM ${client.unsafe(tablename)}
      ORDER BY ${client.unsafe(ascid)} ASC
      LIMIT 10 OFFSET ${client.unsafe(page * 10)}
    `;
    const length = await client`
      SELECT count(*) FROM ${client.unsafe(tablename)}
    `;

    res.send({ message: 'success', data: result, length: length, idname });
  } catch (err) {
    console.log(err);
    res.send({ message: 'failed', data: [] });
  }
});

app.delete('/delete/:tablename', async (req, res) => {
  const { tablename } = req.params;
  try {
    await client`
      DROP TABLE ${client.unsafe(tablename)}
    `;
    res.send({ message: 'success' });
  } catch (err) {
    res.send({ message: 'failed' });
  }
});

app.use(express.json());

app.put('/update/:tablename/:idname/:id', async (req, res) => {
  const { tablename, idname, id } = req.params;
  const data = req.body;

  try {
    const setString = Object.entries(data)
      .map(([key, value]) => `${key} = '${value}'`)
      .join(", ");

    await client`
      UPDATE ${client.unsafe(tablename)}
      SET ${client.unsafe(setString)}
      WHERE ${client.unsafe(idname)} = ${id}
    `;

    res.send({ message: 'success' });
  } catch (err) {
    console.error("Update Error:", err);
    res.send({ message: 'failed' });
  }
});

app.get('/download/:tablename', async (req, res) => {
  const { tablename } = req.params;
  try {
    const data = await client`
      SELECT * FROM ${client.unsafe(tablename)}
    `;
    const pathname = path.join(__dirname, "exports");
    if (!fs.existsSync(pathname)) {
      fs.mkdirSync(pathname, { recursive: true });
    }
    const worksheet = xlsx.utils.json_to_sheet(data);
    const workbook = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(workbook, worksheet, 'sheet 1');
    const filepath = path.join(__dirname, 'exports', `${tablename}-${Date.now()}.xlsx`);
    xlsx.writeFile(workbook, filepath);
    res.download(filepath, `${tablename}.xlsx`, (err) => {
      if (err) {
        res.status(500).json({ message: "Failed to download file" });
      }
    });
  } catch (err) {
    res.send({ message: 'failed' });
  }
});

app.listen(process.env.PORT, () => console.log(`server running on port=${process.env.PORT}`));
