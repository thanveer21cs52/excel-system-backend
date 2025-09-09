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







const allowedOrigins = [
  "http://localhost:3000",  // dev environment
  "https://excel-system-frond-end.vercel.app" // production frontend
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
  const headers = rows[0];
  const firstDataRow = rows[1];

  // Build column definitions (skip id, we add manually)
  const columns = headers.map((col, index) => {
    const value = firstDataRow[index];

    if (typeof value === "number") {
      if (Number.isInteger(value)) {
        if (value >= -2147483648 && value <= 2147483647) {
          return `${col} INT`;
        } else {
          return `${col} BIGINT`;
        }
      } else {
        return `${col} FLOAT`;
      }
    } else {
      return `${col} TEXT`;
    }
  });

  // ✅ Add id column as primary key
  const schema = ["id BIGSERIAL PRIMARY KEY", ...columns].join(", ");

  await client`
    CREATE TABLE IF NOT EXISTS ${client.unsafe(tablename)} (
      ${client.unsafe(schema)}
    )
  `;
}

async function inserttable(tablename, filepath) {
  const rows = await readXlsxFile(filepath);
  const headers = rows[0];

  // Insert each row safely
  for (const row of rows.slice(1)) {
    await client`
      INSERT INTO ${client.unsafe(tablename)} (${client.unsafe(headers.join(','))})
      VALUES (${row})
      ON CONFLICT DO NOTHING
    `;
  }
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
    const header1=await client`
      select * from ${client.unsafe(tablename)} limit 1
     
    `;
  const idname= Object.keys(header1[0])[0]
  

    const result = await client`
      SELECT *
      FROM ${client.unsafe(tablename)}
      order by ${client.unsafe(idname)} asc
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
      length:length,
      idname
    
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

app.use(express.json()); 


app.put('/update/:tablename/:idname/:id', async (req, res) => {
  const { tablename, idname, id } = req.params;
  const data = await req.body;
  console.log("Updating Table:", tablename, "ID:", id, "Data:", data);

  try {
    
    const setString = Object.entries(data)
      .map(([key, value]) => `${key} = '${value}'`)
      .join(", ");
      console.log(setString)

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
