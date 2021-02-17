GO
CREATE DATABASE Conductor;
GO

ALTER DATABASE Conductor
 ADD FILEGROUP fgInMemory contains MEMORY_OPTIMIZED_DATA
GO

ALTER DATABASE Conductor
ADD FILE (
  NAME = 'fInMemory',
  filename = '/var/sqlmem/fInMemory.mdf'
)
to filegroup fgInMemory
GO
