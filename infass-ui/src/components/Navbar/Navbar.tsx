import React from 'react'

export const Navbar = () => {
  return (
    <nav className="bg-white border-b border-gray-200 px-4 py-3 shadow-sm">
      <div className="max-w-7xl mx-auto flex items-center justify-between">
        <input
          type="text"
          placeholder="Search products..."
          className="w-full max-w-md px-4 py-2 border rounded-xl focus:outline-none focus:ring focus:ring-blue-300"
        />
      </div>
    </nav>
  )
}
