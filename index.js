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
async function read(tablename,filepath){
    const rows=await readXlsxFile(filepath)
     arr.push(rows[0]);
   arr.push(rows[1]);
   console.log(arr)
   const data=rows[0].map((col,index)=>{
   const value = rows[1][index];
   console.log(typeof value)

   if (index==0) {
    if(typeof value=='number'){
    
        return `${col} BIGINT PRIMARY KEY`;

      
      

    }
    else{
      return `${col} TEXT PRIMARY KEY`
    }
    
  }
  else if (typeof value === "string") {
    return `${col} TEXT`;
  } else if (typeof value === "number") {
    if (Number.isInteger(value)) {
    
    if (value >= -2147483648 && value <= 2147483647) {
      return `${col} INT`;
    } else {
      return `${col} BIGINT`; 
    }
  } else {
    return `${col} FLOAT`;
  }

  }else if(typeof value === "DATE")
  return `${col} TEXT`
  else {
    return `${col} TEXT`; 
  }

   }).join(",");
   await client`CREATE TABLE IF NOT EXISTS ${client.unsafe(tablename)}(${client.unsafe(data)})`
   
}
async function inserttable(tablename,filepath){
  const rows=await readXlsxFile(filepath)
  const data=rows.slice(1).map(cols=>{
    return `(${cols.map(col=>{
      return typeof col === 'string'
  ? `'${col}'`
  : col instanceof Date
    ? col.toISOString() 
    :  `${col}`;

    }).join(',')}
  )`    
  })
  
const joinedValues=data.join(',')
const header=rows[0].join(',')
console.log(joinedValues,header)
await client`
  INSERT INTO ${client.unsafe(tablename)} (${client.unsafe(header)})
  VALUES ${client.unsafe(joinedValues)}
  ON CONFLICT (${client.unsafe(rows[0][0])}) DO NOTHING
`;



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
