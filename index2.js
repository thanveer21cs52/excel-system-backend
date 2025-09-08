const { neon } = require("@neondatabase/serverless");
const dotenv=require('dotenv')
const readXlsxFile = require('read-excel-file/node')
const multer=require('multer')
dotenv.config()
const arr=[]
const client=neon(process.env.db_url)
const express=require('express')
const app=express()
const path=require('path')
const cors=require('cors')
app.use(cors())
const fs=require('fs')
const xlsx=require('xlsx')

const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, 'uploads/');
  },
  filename: function (req, file, cb) {
    const uniqueSuffix =  file.originalname.split('.')[0]+'-'+Date.now()
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
const upload = multer({ storage: storage,fileFilter });
async function read(tablename, filepath) {
  const rows = await readXlsxFile(filepath);
  if (!rows.length) throw new Error("Empty Excel file");

  
  const safeTableName = tablename.replace(/[^a-zA-Z0-9_]/g, "_");


  const data = rows[0].map((col, index) => {
    const safeCol = `"${col.replace(/\s+/g, "_")}"`; // replace spaces with underscore + quote it
    const value = rows[1][index];

    if (index === 0) {
      return `${safeCol} TEXT PRIMARY KEY`; // always text PK to avoid int/uuid issues
    }
    if (typeof value === "string") return `${safeCol} TEXT`;
    if (typeof value === "number") {
      return Number.isInteger(value)
        ? value >= -2147483648 && value <= 2147483647
          ? `${safeCol} INT`
          : `${safeCol} BIGINT`
        : `${safeCol} FLOAT`;
    }
    if (value instanceof Date) return `${safeCol} TIMESTAMP`;
    return `${safeCol} TEXT`;
  }).join(", ");

   client.unsafe(
    `CREATE TABLE IF NOT EXISTS ${safeTableName} (${data})`
  );

  return safeTableName;
}

async function inserttable(tablename, filepath) {
  const rows = await readXlsxFile(filepath);
  const headers = rows[0].map((col) => `"${col.replace(/\s+/g, "_")}"`).join(", ");

  const data = rows.slice(1).map((cols) => {
    return `(${cols
      .map((col) => {
        if (typeof col === "string")
          return `'${col.replace(/'/g, "''")}'`; // escape single quotes
        if (col instanceof Date)
          return `'${col.toISOString()}'`;
        return col ?? "NULL";
      })
      .join(",")})`;
  });

  if (!data.length) return;

  const joinedValues = data.join(",");

   client.unsafe(`
    INSERT INTO ${tablename} (${headers})
    VALUES ${joinedValues}
    ON CONFLICT (${headers.split(",")[0]}) DO NOTHING
  `);
}

app.post('/upload', upload.single('excel'), async (req, res) => {
  console.log(req.file);
  const filepath=`./uploads/${req.file.filename}`
  const tablename=req.file.originalname.split('.')[0]
  try{
      await read(tablename,filepath)
inserttable(tablename,filepath)
       res.send({message:'success'});

  }
  catch(err){
    console.log(err)
    res.send({message:'failed'});

  }


  
});


app.get('/gettables',async (req,res)=>{

  try {
    const result = await client`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      ORDER BY table_name;
    `;
    res.send({
      message:'success',
      table: result});
  } catch (err) {
    res.send({
      message:'failed',
      table: []})
  }

})
app.get('/tabledata/:tablename/:page',async (req,res)=>{
 const {tablename,page}= req.params
 console.log(tablename)

  try {
    const result = await client`
      SELECT *
      FROM ${client.unsafe(tablename)}
      limit 10
      offset ${client.unsafe(page*10)}
     
    `;
    const length = await client`
      SELECT count(*)
      FROM ${client.unsafe(tablename)}
    
     
    `;

    res.send({
      message:'success',
      data: result,
      length:length
    
    });
  } catch (err) {
    console.log(err)
    res.send({
      message:'failed',
      data: []})
  }

})
app.delete('/delete/:tablename',async (req,res)=>{
 const {tablename}= req.params
 console.log(tablename)

  try {
     await client`
      drop table ${client.unsafe(tablename)}
     
    `;
    res.send({
      message:'success',
  });
  } catch (err) {
    res.send({
      message:'failed',
    })
  }

})
app.get('/download/:tablename',async (req,res)=>{
 const {tablename}= req.params
 console.log(tablename)

  try {
     const data=await client`
      select * from ${client.unsafe(tablename)}
     
    `;
        const pathname = path.join(__dirname, "exports");
  if (!fs.existsSync(pathname)) {
    fs.mkdirSync(pathname, { recursive: true });
  }
  const worksheet=xlsx.utils.json_to_sheet(data)
  const workbook=xlsx.utils.book_new()
  xlsx.utils.book_append_sheet(workbook,worksheet,'sheet 1')
  const filepath=path.join(__dirname,'exports',`${tablename}-${Date.now()}.xlsx`)
  xlsx.writeFile(workbook,filepath)
  res.download(filepath,`${tablename}.xlsx`,(err)=>{
    if(err){
   res.status(500).json({ message: "Failed to download file" });
    }
    else{
      console.log('file downloaded successfully')
    }
  })

  
   
 
  } catch (err) {
    res.send({
      message:'failed',
    })
  }

})

app.listen(process.env.PORT,()=>console.log(`server running on port=${process.env.PORT}`))
